package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// aggrigate ascii grids over 30 years(aggRange) per 1 year (aggStep)
// Number of grids (N)
// generate N/aggStep - aggRange files

const StartYear = 1971 // inclusive
const EndYear = 2099   // inclusive

var crop = map[string]string{
	"WW_rcp85":  "wheatwinterwheat",
	"SM_rcp85":  "maizesilagemaize",
	"WRa_rcp85": "rapewinterrape",
	"WW_rcp26":  "wheatwinterwheat",
	"SM_rcp26":  "maizesilagemaize",
	"WRa_rcp26": "rapewinterrape",
}

var scenario = map[string]string{
	"WW_rcp85":  "rcp85",
	"SM_rcp85":  "rcp85",
	"WRa_rcp85": "rcp85",
	"WW_rcp26":  "rcp26",
	"SM_rcp26":  "rcp26",
	"WRa_rcp26": "rcp26",
}

// hard coded setup map
var setups = map[string][]int{
	"WW_rcp85":  {1}, // 41, 65, 81, 121, 129},
	"SM_rcp85":  {7, 47, 71, 87, 127, 135},
	"WRa_rcp85": {4, 44, 68, 84, 124, 132},
	"WW_rcp26":  {9, 17, 57, 89, 97},
	"SM_rcp26":  {15, 23, 63, 95, 103},
	"WRa_rcp26": {12, 20, 60, 92, 100},
}

const inputFileformat = "%s_Yield_%d_%d.asc"
const outfileTemplate = "%s_avgYield_%s_%d_%d.asc"         // crop[setupId], scenario[setupId], imageYear, index
const outDiffFileTemplate = "%s_avgYieldDiff_%s_%d_%d.asc" // crop[setupId], scenario[setupId], imageYear, index

func main() {
	inputFolderPtr := flag.String("in", "./test", "path to input")
	outFolderPtr := flag.String("out", "./agg_out", "path to output")
	concurrentPtr := flag.Int("concurrent", 1, "max concurrent execution")
	aggRangePtr := flag.Uint("aggRange", 30, "avarage of n years (default 30)")
	aggStepPtr := flag.Uint("aggStep", 1, "year jumps (default 1)")
	setupIdPtr := flag.String("setup", "WW_rcp85", "setup id")

	flag.Parse()
	inputFolder := *inputFolderPtr
	outFolder := *outFolderPtr
	numConcurrent := *concurrentPtr

	aggRange := *aggRangePtr
	aggStep := *aggStepPtr
	setupId := *setupIdPtr

	if aggRange%2 > 0 {
		log.Fatal("aggRange should be an even number")
	}
	if _, ok := setups[setupId]; !ok {
		log.Fatal("setup id not found")
	}

	aggRangeHalf := aggRange / 2
	startIdx := StartYear + aggRange/2
	endIdx := EndYear - aggRange/2
	imageYearIndex := 0
	gMinMax := newMinMax()
	outChan := make(chan MinMax)
	currRuns := 0
	var refGrid [][]float64
	for imageYear := startIdx; imageYear < endIdx; imageYear = imageYear + aggStep {
		imageYearIndex++

		calcAvgGrid := func(imageYear uint, imageYearIndex int, outC chan MinMax) (currentYearGrid [][]float64, mMinMax MinMax) {

			yearCounter := 0
			var header map[string]float64
			nodata := -1.0
			for _, setup := range setups[setupId] {
				// load grid - to grid buffer
				for imageIdx := imageYear - aggRangeHalf; imageIdx < imageYear+aggRangeHalf; imageIdx++ {
					// read grid file
					index := imageIdx - StartYear + 1
					filepath := filepath.Join(inputFolder, strconv.Itoa(setup), fmt.Sprintf(inputFileformat, crop[setupId], imageIdx, index))
					file, err := os.Open(filepath)
					if err != nil {
						log.Fatal(err)
					}
					scanner := bufio.NewScanner(file)

					if yearCounter == 0 {
						// read header, init currentGrid
						header = readHeader(scanner, false)
						cols := int(header["ncols"])
						rows := int(header["nrows"])
						nodata = header["nodata_value"]
						currentYearGrid = make([][]float64, rows)
						for row := 0; row < rows; row++ {
							currentYearGrid[row] = make([]float64, cols)
						}
					} else {
						// skip first lines
						readHeader(scanner, true)
					}
					currRow := 0
					for scanner.Scan() {
						// sum up grid cells
						fields := strings.Fields(scanner.Text())
						for i, field := range fields {
							val, err := strconv.ParseFloat(field, 32)
							if err != nil {
								log.Fatal(err)
							}
							if val-nodata < 0.001 {
								currentYearGrid[currRow][i] = nodata
							} else {
								currentYearGrid[currRow][i] = currentYearGrid[currRow][i] + val
							}

						}
						currRow++
					}

					file.Close()
					yearCounter++
				}
			}
			mMinMax = newMinMax()
			// calc average
			for rowIdx, row := range currentYearGrid {
				for colIdx, col := range row {
					if currentYearGrid[rowIdx][colIdx] != nodata {
						currentYearGrid[rowIdx][colIdx] = col / float64(yearCounter)
						mMinMax.setMax(int(currentYearGrid[rowIdx][colIdx]))
						mMinMax.setMin(int(currentYearGrid[rowIdx][colIdx]))
					}
				}
			}
			// save new grid
			outFileName := filepath.Join(outFolder, fmt.Sprintf(outfileTemplate, crop[setupId], scenario[setupId], imageYear, imageYearIndex))
			fout := writeAGridHeader(outFileName, header)
			writeFloatRows(fout, currentYearGrid)
			fout.Close()
			if refGrid != nil {
				diffgrid := createDiff(refGrid, currentYearGrid, nodata)

				outDiffFileName := filepath.Join(outFolder, fmt.Sprintf(outDiffFileTemplate, crop[setupId], scenario[setupId], imageYear, imageYearIndex))
				fout := writeAGridHeader(outDiffFileName, header)
				writeIntRows(fout, diffgrid)
				fout.Close()
			}
			if outC != nil {
				outC <- mMinMax
			}

			return currentYearGrid, mMinMax
		}

		if imageYearIndex == 1 {
			// read reference grid (historical data) for diff maps
			refGrid, gMinMax = calcAvgGrid(imageYear, imageYearIndex, nil)
		} else {
			go calcAvgGrid(imageYear, imageYearIndex, outChan)

			currRuns++
			if currRuns >= numConcurrent {
				for currRuns >= numConcurrent {
					mMM := <-outChan
					currRuns--
					gMinMax.setMax(mMM.maxYield)
					gMinMax.setMin(mMM.minYield)

				}
			}
		}
	}
	for currRuns > 0 {
		mMM := <-outChan
		currRuns--
		gMinMax.setMax(mMM.maxYield)
		gMinMax.setMin(mMM.minYield)

	}
}

func createDiff(refGrid, currentYearGrid [][]float64, nodata float64) (diffgrid [][]int) {

	diffgrid = make([][]int, len(currentYearGrid))

	calcDiffValue := func(hist, future int) int {
		diffVal := 0
		// catch diff by 0
		if hist == 0 {
			if future > 0 {
				diffVal = 100
			}
		} else {
			diffVal = (future - hist) * 100 / hist
		}
		// cap at +/-100%
		if diffVal > 100 {
			diffVal = 101
		}
		return diffVal
	}

	for rowIdx, row := range currentYearGrid {
		diffgrid[rowIdx] = make([]int, len(row))
		for colIdx := range row {
			if currentYearGrid[rowIdx][colIdx] == nodata {
				diffgrid[rowIdx][colIdx] = int(nodata)
			} else {
				diffgrid[rowIdx][colIdx] = calcDiffValue(int(refGrid[rowIdx][colIdx]), int(currentYearGrid[rowIdx][colIdx]))
			}
		}
	}
	return diffgrid
}

// readHeader and return a map of values, or skip the header
func readHeader(scanner *bufio.Scanner, skip bool) map[string]float64 {
	currRow := 0
	if skip {
		for currRow < 6 && scanner.Scan() {
			currRow++
		}
		return nil
	}
	outMap := make(map[string]float64, 6)

	for currRow < 6 && scanner.Scan() {
		currRow++
		texts := strings.Fields(scanner.Text())
		val, err := strconv.ParseFloat(texts[1], 32)
		if err != nil {
			log.Fatal(err)
		}
		outMap[texts[0]] = val
	}
	return outMap
}

// Fout combined file writer
type Fout struct {
	file    *os.File
	gfile   *gzip.Writer
	fwriter *bufio.Writer
}

// Write string to zip file
func (f Fout) Write(s string) {
	f.fwriter.WriteString(s)
}

// Close file writer
func (f Fout) Close() {
	f.fwriter.Flush()
	// Close the gzip first.
	f.gfile.Close()
	f.file.Close()
}

func writeAGridHeader(name string, header map[string]float64) (fout Fout) {
	cornerX := header["xllcorner"]
	cornery := header["yllcorner"]
	novalue := int(header["nodata_value"])
	cellsize := header["cellsize"]
	nCol := int(header["ncols"])
	nRow := int(header["nrows"])
	// create an ascii file, which contains the header
	makeDir(name)
	file, err := os.OpenFile(name+".gz", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}

	gfile := gzip.NewWriter(file)
	fwriter := bufio.NewWriter(gfile)
	fout = Fout{file, gfile, fwriter}

	fout.Write(fmt.Sprintf("ncols %d\n", nCol))
	fout.Write(fmt.Sprintf("nrows %d\n", nRow))
	fout.Write(fmt.Sprintf("xllcorner     %f\n", cornerX))
	fout.Write(fmt.Sprintf("yllcorner     %f\n", cornery))
	fout.Write(fmt.Sprintf("cellsize      %f\n", cellsize))
	fout.Write(fmt.Sprintf("NODATA_value  %d\n", novalue))

	return fout
}

func writeFloatRows(fout Fout, grid [][]float64) {

	for _, row := range grid {
		for _, col := range row {
			fout.Write(strconv.Itoa(int(col)))
			fout.Write(" ")
		}
		fout.Write("\n")
	}
}

func writeIntRows(fout Fout, grid [][]int) {

	for _, row := range grid {
		for _, col := range row {
			fout.Write(strconv.Itoa(col))
			fout.Write(" ")
		}
		fout.Write("\n")
	}
}

func makeDir(outPath string) {
	dir := filepath.Dir(outPath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			log.Fatalf("ERROR: Failed to generate output path %s :%v", dir, err)
		}
	}
}

func writeMetaFile(gridFilePath, title, labeltext, colormap string, colorlist []string, cbarLabel []string, ticklist []float64, factor float64, maxValue, minValue, nodata int, minColor string) {
	metaFilePath := gridFilePath + ".meta"
	makeDir(metaFilePath)
	file, err := os.OpenFile(metaFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.WriteString(fmt.Sprintf("title: '%s'\n", title))
	file.WriteString(fmt.Sprintf("labeltext: '%s'\n", labeltext))
	if colormap != "" {
		file.WriteString(fmt.Sprintf("colormap: '%s'\n", colormap))
	}
	if colorlist != nil {
		file.WriteString("colorlist: \n")
		for _, item := range colorlist {
			file.WriteString(fmt.Sprintf(" - '%s'\n", item))
		}
	}
	if cbarLabel != nil {
		file.WriteString("cbarLabel: \n")
		for _, cbarItem := range cbarLabel {
			file.WriteString(fmt.Sprintf(" - '%s'\n", cbarItem))
		}
	}
	if ticklist != nil {
		file.WriteString("ticklist: \n")
		for _, tick := range ticklist {
			file.WriteString(fmt.Sprintf(" - %f\n", tick))
		}
	}
	file.WriteString(fmt.Sprintf("factor: %f\n", factor))
	if maxValue != nodata {
		file.WriteString(fmt.Sprintf("maxValue: %d\n", maxValue))
	}
	if minValue != nodata {
		file.WriteString(fmt.Sprintf("minValue: %d\n", minValue))
	}
	if len(minColor) > 0 {
		file.WriteString(fmt.Sprintf("minColor: %s\n", minColor))
	}
}

type MinMax struct {
	minYield int
	maxYield int
}

func newMinMax() MinMax {
	return MinMax{
		minYield: -1,
		maxYield: -1,
	}
}

func (m *MinMax) setMin(min int) {
	if m.minYield > min || m.minYield < 0 {
		m.minYield = min
	}
}
func (m *MinMax) setMax(max int) {
	if m.maxYield < max || m.maxYield < 0 {
		m.maxYield = max
	}
}
