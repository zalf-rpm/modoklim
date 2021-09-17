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

	"gonum.org/v1/gonum/stat"
)

// aggrigate ascii grids over 30 years(aggRange) per 1 year (aggStep)
// Number of grids (N)
// generate N/aggStep - aggRange files

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
	"WW_rcp85":  {1, 41, 65, 81, 121, 129},
	"SM_rcp85":  {7, 47, 71, 87, 127, 135},
	"WRa_rcp85": {4, 44, 68, 84, 124, 132},
	"WW_rcp26":  {9, 17, 57, 89, 97},
	"SM_rcp26":  {15, 23, 63, 95, 103},
	"WRa_rcp26": {12, 20, 60, 92, 100},
}

const inputFileformat = "%s_Yield_%d_%d.asc"
const outfileTemplate = "avgYield_%s_%s_%d_%d.asc"         // crop[setupId], scenario[setupId], imageYear, index
const outDiffFileTemplate = "avgYieldDiff_%s_%s_%d_%d.asc" // crop[setupId], scenario[setupId], imageYear, index
const outStdfileTemplate = "avgYieldStd_%s_%s_%d_%d.asc"   // crop[setupId], scenario[setupId], imageYear, index

var inputFolder = "./test"
var outFolder = "./agg_out"
var numConcurrent = 10
var aggRange uint = 30
var aggStep uint = 1
var cropId = "WW"
var startYear uint = 1971 // inclusive
var endYear uint = 2099   // inclusive

func main() {
	inputFolderPtr := flag.String("in", inputFolder, "path to input")
	outFolderPtr := flag.String("out", outFolder, "path to output")
	concurrentPtr := flag.Int("concurrent", numConcurrent, "max concurrent execution")
	aggRangePtr := flag.Uint("aggRange", aggRange, "avarage of n years (default 30)")
	aggStepPtr := flag.Uint("aggStep", aggStep, "year jumps (default 1)")
	startyearPtr := flag.Uint("start", startYear, "start year (inclusive)")
	endyearPtr := flag.Uint("end", endYear, "end year (inclusive)")
	cropIdPtr := flag.String("crop", cropId, "crop id")

	flag.Parse()
	inputFolder = *inputFolderPtr
	outFolder = *outFolderPtr
	numConcurrent = *concurrentPtr

	aggRange = *aggRangePtr
	aggStep = *aggStepPtr
	cropId = *cropIdPtr
	startYear = *startyearPtr
	endYear = *endyearPtr

	if aggRange%2 > 0 {
		log.Fatal("aggRange should be an even number")
	}
	setupIds := []string{}
	for key := range setups {
		if strings.HasPrefix(key, cropId) {
			setupIds = append(setupIds, key)
		}
	}
	if len(setupIds) == 0 {
		log.Fatal("no setups founf for crop")
	}

	startIdx := startYear + aggRange/2
	endIdx := endYear - aggRange/2

	gMinMax := newMinMax()
	outChan := make(chan MinMax)
	fileNameChan := make(chan string)
	final := make(chan MinMax)
	terminate := make(chan bool)
	go filenameCollector(fileNameChan, final, terminate)

	currRuns := 0

	for _, setupId := range setupIds {
		imageYearIndex := 0
		var refGrid [][]float64
		for imageYear := startIdx; imageYear < endIdx; imageYear = imageYear + aggStep {
			imageYearIndex++

			if imageYearIndex == 1 {
				// read reference grid (historical data) for diff maps
				var lMinMax MinMax
				refGrid, lMinMax = calcAvgGrid(nil, setupId, imageYear, imageYearIndex, fileNameChan, nil)
				gMinMax.setMax(lMinMax.maxYield)
				gMinMax.setMin(lMinMax.minYield)
				gMinMax.setStdMax(lMinMax.maxStd)
				gMinMax.setStdMin(lMinMax.minStd)
			} else {
				go calcAvgGrid(refGrid, setupId, imageYear, imageYearIndex, fileNameChan, outChan)

				currRuns++
				if currRuns >= numConcurrent {
					for currRuns >= numConcurrent {
						mMM := <-outChan
						currRuns--
						gMinMax.setMax(mMM.maxYield)
						gMinMax.setMin(mMM.minYield)
						gMinMax.setStdMax(mMM.maxStd)
						gMinMax.setStdMin(mMM.minStd)

					}
				}
			}
		}
		for currRuns > 0 {
			mMM := <-outChan
			currRuns--
			gMinMax.setMax(mMM.maxYield)
			gMinMax.setMin(mMM.minYield)
			gMinMax.setStdMax(mMM.maxStd)
			gMinMax.setStdMin(mMM.minStd)
		}
	}

	// send final min max values
	final <- gMinMax
	// wait for termination of meta file writing
	<-terminate
}

func filenameCollector(in chan string, final chan MinMax, out chan bool) {
	diffFilenames := []string{}
	yieldFilenames := []string{}
	stdFilenames := []string{}

	for {
		select {
		case globalMinMax := <-final:
			// run finished create meta files
			createMeta(yieldFilenames, diffFilenames, stdFilenames, globalMinMax)
			// send finish signal to terminate
			out <- true
			return
		case filename := <-in:
			if strings.Contains(filename, "Diff") {
				diffFilenames = append(diffFilenames, filename)
			} else if strings.Contains(filename, "Std") {
				stdFilenames = append(stdFilenames, filename)
			} else {
				yieldFilenames = append(yieldFilenames, filename)
			}
		}
	}
}

func calcAvgGrid(refGrid [][]float64, setupId string, imageYear uint, imageYearIndex int, fileNameChan chan string, outC chan MinMax) (currentYearGrid [][]float64, mMinMax MinMax) {

	aggRangeHalf := aggRange / 2
	yearCounter := 0
	var header map[string]float64
	nodata := -1.0
	var stdScenGrid [][][]float64
	var stdeviationGrid [][]float64
	for _, setup := range setups[setupId] {

		// load grid - to grid buffer
		for imageIdx := imageYear - aggRangeHalf; imageIdx < imageYear+aggRangeHalf; imageIdx++ {
			// read grid file
			index := imageIdx - startYear + 1
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
				stdeviationGrid = make([][]float64, rows)

				stdScenGrid = make([][][]float64, rows)
				for row := 0; row < rows; row++ {
					currentYearGrid[row] = make([]float64, cols)
					stdeviationGrid[row] = make([]float64, cols)
					stdScenGrid[row] = make([][]float64, cols)
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
						if stdScenGrid[currRow][i] == nil {
							stdScenGrid[currRow][i] = make([]float64, 0, aggRange)
						}
						stdScenGrid[currRow][i] = append(stdScenGrid[currRow][i], val)
					}
				}
				currRow++
			}

			file.Close()
			yearCounter++
		}
		for rowIdx, row := range stdScenGrid {
			for colIdx := range row {
				stdeviationGrid[rowIdx][colIdx] = stat.StdDev(stdScenGrid[rowIdx][colIdx], nil)
				stdScenGrid[rowIdx][colIdx] = nil
			}
		}
	}
	mMinMax = newMinMax()
	// calc average
	for rowIdx, row := range currentYearGrid {
		for colIdx, col := range row {
			if currentYearGrid[rowIdx][colIdx] != nodata {
				currentYearGrid[rowIdx][colIdx] = col / float64(yearCounter)
				stdeviationGrid[rowIdx][colIdx] = stdeviationGrid[rowIdx][colIdx] / float64(len(setups[setupId]))

				mMinMax.setStdMax(int(stdeviationGrid[rowIdx][colIdx]))
				mMinMax.setStdMin(int(stdeviationGrid[rowIdx][colIdx]))
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
	fileNameChan <- outFileName
	if refGrid != nil {
		diffgrid := createDiff(refGrid, currentYearGrid, nodata)

		outDiffFileName := filepath.Join(outFolder, fmt.Sprintf(outDiffFileTemplate, crop[setupId], scenario[setupId], imageYear, imageYearIndex))
		fout := writeAGridHeader(outDiffFileName, header)
		writeIntRows(fout, diffgrid)
		fout.Close()
		fileNameChan <- outDiffFileName
	}
	outstdFileName := filepath.Join(outFolder, fmt.Sprintf(outStdfileTemplate, crop[setupId], scenario[setupId], imageYear, imageYearIndex))
	foutStd := writeAGridHeader(outstdFileName, header)
	writeFloatRows(foutStd, stdeviationGrid)
	foutStd.Close()
	fileNameChan <- outstdFileName

	if outC != nil {
		outC <- mMinMax
	}

	return currentYearGrid, mMinMax
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

func createMeta(yieldFilelist, diffFileList, stdFilelist []string, globalMinMax MinMax) {

	yieldMeta := newYieldMetaSetup(globalMinMax.minYield, globalMinMax.maxYield)
	diffMeta := newDiffMetaSet()
	stdMeta := newStdMetaSet(globalMinMax.minStd, globalMinMax.maxStd)
	for _, filename := range yieldFilelist {
		writeMetaFile(filename, yieldMeta)
	}
	for _, filename := range diffFileList {
		writeMetaFile(filename, diffMeta)
	}
	for _, filename := range stdFilelist {
		writeMetaFile(filename, stdMeta)
	}
}

type metaSetup struct {
	colormap string
	maxValue int
	minValue int

	minColor string
}

func newDiffMetaSet() metaSetup {
	return metaSetup{
		colormap: "RdYlGn",
		maxValue: 101,
		minValue: -101,
		minColor: "lightgrey",
	}
}

func newYieldMetaSetup(minValue, maxValue int) metaSetup {
	return metaSetup{
		colormap: "jet",
		maxValue: maxValue,
		minValue: minValue,
		minColor: "lightgrey",
	}
}
func newStdMetaSet(minValue, maxValue int) metaSetup {
	return metaSetup{
		colormap: "cool",
		maxValue: maxValue,
		minValue: minValue,
		minColor: "lightgrey",
	}
}

func writeMetaFile(gridFilePath string, setup metaSetup) {
	metaFilePath := gridFilePath + ".meta"
	makeDir(metaFilePath)
	file, err := os.OpenFile(metaFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	if setup.colormap != "" {
		file.WriteString(fmt.Sprintf("colormap: '%s'\n", setup.colormap))
	}
	file.WriteString(fmt.Sprintf("maxValue: %d\n", setup.maxValue))
	file.WriteString(fmt.Sprintf("minValue: %d\n", setup.minValue))
	if len(setup.minColor) > 0 {
		file.WriteString(fmt.Sprintf("minColor: %s\n", setup.minColor))
	}
}

type MinMax struct {
	minYield int
	maxYield int
	minStd   int
	maxStd   int
}

func newMinMax() MinMax {
	return MinMax{
		minYield: -1,
		maxYield: -1,
		minStd:   -1,
		maxStd:   -1,
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
func (m *MinMax) setStdMin(min int) {
	if m.minStd > min || m.minStd < 0 {
		m.minStd = min
	}
}
func (m *MinMax) setStdMax(max int) {
	if m.maxStd < max || m.maxStd < 0 {
		m.maxStd = max
	}
}
