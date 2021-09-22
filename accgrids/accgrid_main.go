package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"gonum.org/v1/gonum/stat"
)

// aggrigate ascii grids over 30 years(aggRange) per 1 year (aggStep)
// Number of grids (N)
// generate N/aggStep - aggRange files

var crop = map[string]string{
	"WW_rcp85":      "wheatwinterwheat",
	"SM_rcp85":      "maizesilagemaize",
	"WRa_rcp85":     "rapewinterrape",
	"WW_rcp26":      "wheatwinterwheat",
	"SM_rcp26":      "maizesilagemaize",
	"WRa_rcp26":     "rapewinterrape",
	"WW_rcp85_420":  "wheatwinterwheat",
	"WRa_rcp85_420": "rapewinterrape",
	"SM_rcp85_420":  "maizesilagemaize",
}

var scenario = map[string]string{
	"WW_rcp85":      "rcp85",
	"SM_rcp85":      "rcp85",
	"WRa_rcp85":     "rcp85",
	"WW_rcp26":      "rcp26",
	"SM_rcp26":      "rcp26",
	"WRa_rcp26":     "rcp26",
	"WW_rcp85_420":  "rcp85",
	"WRa_rcp85_420": "rcp85",
	"SM_rcp85_420":  "rcp85",
}

var co2 = map[string]string{
	"WW_rcp85":      "-",
	"WRa_rcp85":     "-",
	"SM_rcp85":      "-",
	"WW_rcp85_420":  "420",
	"WRa_rcp85_420": "420",
	"SM_rcp85_420":  "420",
}

// hard coded setup map
// var setups = map[string][]int{
// 	"WW_rcp85":  {1, 41, 65, 81, 121, 129},
// 	"SM_rcp85":  {7, 47, 71, 87, 127, 135},
// 	"WRa_rcp85": {4, 44, 68, 84, 124, 132},
// 	"WW_rcp26":  {9, 17, 57, 89, 97},
// 	"SM_rcp26":  {15, 23, 63, 95, 103},
// 	"WRa_rcp26": {12, 20, 60, 92, 100},
// }

// da sims
var setups = map[string][]int{
	"WW_rcp85":      {178, 182, 186, 190, 194, 198},
	"WRa_rcp85":     {180, 184, 188, 192, 196, 200},
	"SM_rcp85":      {181, 185, 189, 193, 197, 201},
	"WW_rcp85_420":  {202, 206, 210, 214, 218, 222},
	"WRa_rcp85_420": {204, 208, 212, 216, 220, 224},
	"SM_rcp85_420":  {205, 209, 213, 217, 221, 225},
}

const inputFileformat = "%s_Yield_%d_%d.asc"

const inputFileformatPotET = "%s_Pot_ET_%d_%d.asc"
const inputFileformatPrecipSum = "%s_precip-sum_%d_%d.asc"
const inputFileformatTavgAvg = "%s_tavg-avg_%d_%d.asc"
const inputFileformatTmaxAvg = "%s_tmax-avg_%d_%d.asc"
const inputFileformatTminAvg = "%s_tmin-avg_%d_%d.asc"

const outStdfileTemplate = "avgYieldStd_%s_%s_%s_%d_%d.asc" // crop[setupId], scenario[setupId], co2, imageYear, index

// crop[setupId], scenario[setupId], co2, imageYear, index
var outDiffFileTemplate = map[refGridName]string{
	yields:    "avgYieldDiff_%s_%s_%s_%d_%d.asc",
	potET:     "avgPot_ETDiff_%s_%s_%s_%d_%d.asc",
	precipSum: "avgprecip-sumDiff_%s_%s_%s_%d_%d.asc",
	tavgAvg:   "avgtavg-avgDiff_%s_%s_%s_%d_%d.asc",
	tmaxAvg:   "avgtmax-avgDiff_%s_%s_%s_%d_%d.asc",
	tminAvg:   "avgtmin-avgDiff_%s_%s_%s_%d_%d.asc",
}

// crop[setupId], scenario[setupId], co2, imageYear, index
var outfileTemplate = map[refGridName]string{
	yields:    "avgYield_%s_%s_%s_%d_%d.asc",
	potET:     "Pot_ET_%s_%s_%s_%d_%d.asc",
	precipSum: "precip-sum_%s_%s_%s_%d_%d.asc",
	tavgAvg:   "tavg-avg_%s_%s_%s_%d_%d.asc",
	tmaxAvg:   "tmax-avg_%s_%s_%s_%d_%d.asc",
	tminAvg:   "tmin-avg_%s_%s_%s_%d_%d.asc",
}

var inputFolder = "./test"
var outFolder = "./agg_out"
var numConcurrent = 1
var aggRange uint = 30
var aggStep uint = 1
var cropId = "WW"
var startYear uint = 1971 // inclusive
var endYear uint = 2099   // inclusive
var withClimate = true

func main() {
	inputFolderPtr := flag.String("in", inputFolder, "path to input")
	outFolderPtr := flag.String("out", outFolder, "path to output")
	concurrentPtr := flag.Int("concurrent", numConcurrent, "max concurrent execution")
	aggRangePtr := flag.Uint("aggRange", aggRange, "avarage of n years (default 30)")
	aggStepPtr := flag.Uint("aggStep", aggStep, "year jumps (default 1)")
	startyearPtr := flag.Uint("start", startYear, "start year (inclusive)")
	endyearPtr := flag.Uint("end", endYear, "end year (inclusive)")
	cropIdPtr := flag.String("crop", cropId, "crop id")
	withClimPtr := flag.Bool("climate", withClimate, "with climate")

	flag.Parse()
	inputFolder = *inputFolderPtr
	outFolder = *outFolderPtr
	numConcurrent = *concurrentPtr
	withClimate = *withClimPtr

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
	endIdx := endYear - aggRange/2 + 1

	gMinMax := newMinMax()
	outChan := make(chan MinMax)
	fileNameChan := make(chan string)
	final := make(chan MinMax)
	terminate := make(chan bool)
	go filenameCollector(fileNameChan, final, terminate)

	currRuns := 0

	for _, setupId := range setupIds {
		imageYearIndex := 0
		var refGrids map[refGridName][][]float64

		for imageYear := startIdx; imageYear < endIdx; imageYear = imageYear + aggStep {
			imageYearIndex++

			if imageYearIndex == 1 {
				// read reference grid (historical data) for diff maps
				lMinMax := calcAvgGrid(refGrids, withClimate, setupId, imageYear, imageYearIndex, fileNameChan, nil)
				gMinMax.setMaxV(int64(lMinMax.maxYield), "maxYield")
				gMinMax.setMinV(int64(lMinMax.minYield), "minYield")
				gMinMax.setMaxV(int64(lMinMax.maxStd), "maxStd")
				gMinMax.setMinV(int64(lMinMax.minStd), "minStd")
			} else {
				go calcAvgGrid(refGrids, withClimate, setupId, imageYear, imageYearIndex, fileNameChan, outChan)

				currRuns++
				if currRuns >= numConcurrent {
					for currRuns >= numConcurrent {
						mMM := <-outChan
						currRuns--
						gMinMax.setMaxV(int64(mMM.maxYield), "maxYield")
						gMinMax.setMinV(int64(mMM.minYield), "minYield")
						gMinMax.setMaxV(int64(mMM.maxStd), "maxStd")
						gMinMax.setMinV(int64(mMM.minStd), "minStd")

					}
				}
			}
		}
		for currRuns > 0 {
			mMM := <-outChan
			currRuns--
			gMinMax.setMaxV(int64(mMM.maxYield), "maxYield")
			gMinMax.setMinV(int64(mMM.minYield), "minYield")
			gMinMax.setMaxV(int64(mMM.maxStd), "maxStd")
			gMinMax.setMinV(int64(mMM.minStd), "minStd")

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

type refGridName int

const (
	yields refGridName = iota
	potET
	precipSum
	tavgAvg
	tmaxAvg
	tminAvg
)

func calcAvgGrid(refGrids map[refGridName][][]float64, withClimate bool, setupId string, imageYear uint, imageYearIndex int, fileNameChan chan string, outC chan MinMax) (mMinMax MinMax) {

	var currentYearGrid [][]float64
	var currPotETGRid [][]float64
	var currPrecipSumGRid [][]float64
	var currTavgAvgGrid [][]float64
	var currTmaxAvgGrid [][]float64
	var currTminAvgGrid [][]float64

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

			readFile := func(inFileformat string, grid [][]float64, stdGrid [][][]float64) {
				filepath := filepath.Join(inputFolder, strconv.Itoa(setup), fmt.Sprintf(inFileformat, crop[setupId], imageIdx, index))
				file, err := os.Open(filepath)
				if err != nil {
					log.Fatal(err)
				}
				defer file.Close()
				scanner := bufio.NewScanner(file)

				if header == nil {
					header = readHeader(scanner, false)
					cols := int(header["ncols"])
					rows := int(header["nrows"])
					nodata = header["nodata_value"]
					grid = make([][]float64, rows)
					stdGrid = make([][][]float64, rows)
					for row := 0; row < rows; row++ {
						grid[row] = make([]float64, cols)
						stdGrid[row] = make([][]float64, cols)
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
							grid[currRow][i] = nodata
						} else {
							grid[currRow][i] = grid[currRow][i] + val
							if stdGrid[currRow][i] == nil {
								stdGrid[currRow][i] = make([]float64, 0, aggRange)
							}
							stdGrid[currRow][i] = append(stdGrid[currRow][i], val)
						}
					}
					currRow++
				}
			}
			readFile(inputFileformat, currentYearGrid, stdScenGrid)

			stdeviationGrid = make([][]float64, int(header["nrows"]))

			for rowIdx, row := range stdScenGrid {
				if stdeviationGrid[rowIdx] == nil {
					stdeviationGrid[rowIdx] = make([]float64, len(stdScenGrid[rowIdx]))
				}
				for colIdx := range row {
					stdeviationGrid[rowIdx][colIdx] = stdeviationGrid[rowIdx][colIdx] + stat.StdDev(stdScenGrid[rowIdx][colIdx], nil)
				}
			}
			cleanStdDev := func() {
				for rowIdx, row := range stdScenGrid {
					for colIdx := range row {
						stdScenGrid[rowIdx][colIdx] = nil
					}
				}
			}
			cleanStdDev()
			if withClimate {
				readFile(inputFileformatPotET, currPotETGRid, stdScenGrid)
				cleanStdDev()
				readFile(inputFileformatPrecipSum, currPrecipSumGRid, stdScenGrid)
				cleanStdDev()
				readFile(inputFileformatTavgAvg, currTavgAvgGrid, stdScenGrid)
				cleanStdDev()
				readFile(inputFileformatTmaxAvg, currTmaxAvgGrid, stdScenGrid)
				cleanStdDev()
				readFile(inputFileformatTminAvg, currTminAvgGrid, stdScenGrid)
				cleanStdDev()
			}

			yearCounter++
			// 	filepath := filepath.Join(inputFolder, strconv.Itoa(setup), fmt.Sprintf(inputFileformat, crop[setupId], imageIdx, index))
			// 	file, err := os.Open(filepath)
			// 	if err != nil {
			// 		log.Fatal(err)
			// 	}
			// 	scanner := bufio.NewScanner(file)

			// 	if yearCounter == 0 {
			// 		// read header, init currentGrid
			// 		header = readHeader(scanner, false)
			// 		cols := int(header["ncols"])
			// 		rows := int(header["nrows"])
			// 		nodata = header["nodata_value"]
			// 		currentYearGrid = make([][]float64, rows)
			// 		stdeviationGrid = make([][]float64, rows)

			// 		currPotETGRid = make([][]float64, rows)
			// 		currPrecipSumGRid = make([][]float64, rows)
			// 		currTavgAvgGrid = make([][]float64, rows)
			// 		currTmaxAvgGrid = make([][]float64, rows)
			// 		currTminAvgGrid = make([][]float64, rows)

			// 		stdScenGrid = make([][][]float64, rows)
			// 		for row := 0; row < rows; row++ {
			// 			currentYearGrid[row] = make([]float64, cols)
			// 			currPotETGRid[row] = make([]float64, cols)
			// 			currPrecipSumGRid[row] = make([]float64, cols)
			// 			currTavgAvgGrid[row] = make([]float64, cols)
			// 			currTmaxAvgGrid[row] = make([]float64, cols)
			// 			currTminAvgGrid[row] = make([]float64, cols)
			// 			stdeviationGrid[row] = make([]float64, cols)
			// 			stdScenGrid[row] = make([][]float64, cols)
			// 		}
			// 	} else {
			// 		// skip first lines
			// 		readHeader(scanner, true)
			// 	}
			// 	currRow := 0
			// 	for scanner.Scan() {
			// 		// sum up grid cells
			// 		fields := strings.Fields(scanner.Text())
			// 		for i, field := range fields {
			// 			val, err := strconv.ParseFloat(field, 32)
			// 			if err != nil {
			// 				log.Fatal(err)
			// 			}
			// 			if val-nodata < 0.001 {
			// 				currentYearGrid[currRow][i] = nodata
			// 			} else {
			// 				currentYearGrid[currRow][i] = currentYearGrid[currRow][i] + val
			// 				if stdScenGrid[currRow][i] == nil {
			// 					stdScenGrid[currRow][i] = make([]float64, 0, aggRange)
			// 				}
			// 				stdScenGrid[currRow][i] = append(stdScenGrid[currRow][i], val)
			// 			}
			// 		}
			// 		currRow++
			// 	}

			// 	file.Close()
			// 	yearCounter++
			// }
			// for rowIdx, row := range stdScenGrid {
			// 	for colIdx := range row {
			// 		stdeviationGrid[rowIdx][colIdx] = stat.StdDev(stdScenGrid[rowIdx][colIdx], nil)
			// 		stdScenGrid[rowIdx][colIdx] = nil
			// 	}
		}
	}
	mMinMax = newMinMax()
	// calc average
	for rowIdx, row := range currentYearGrid {
		for colIdx, col := range row {
			if currentYearGrid[rowIdx][colIdx] != nodata {
				currentYearGrid[rowIdx][colIdx] = col / float64(yearCounter)
				stdeviationGrid[rowIdx][colIdx] = stdeviationGrid[rowIdx][colIdx] / float64(len(setups[setupId]))

				mMinMax.setMaxV(int64(stdeviationGrid[rowIdx][colIdx]), "maxStd")
				mMinMax.setMinV(int64(stdeviationGrid[rowIdx][colIdx]), "minStd")
				mMinMax.setMaxV(int64(currentYearGrid[rowIdx][colIdx]), "maxYield")
				mMinMax.setMinV(int64(currentYearGrid[rowIdx][colIdx]), "minYield")
				if withClimate {
					currPotETGRid[rowIdx][colIdx] = currPotETGRid[rowIdx][colIdx] / float64(yearCounter)
					currPrecipSumGRid[rowIdx][colIdx] = currPrecipSumGRid[rowIdx][colIdx] / float64(yearCounter)
					currTavgAvgGrid[rowIdx][colIdx] = currTavgAvgGrid[rowIdx][colIdx] / float64(yearCounter)
					currTmaxAvgGrid[rowIdx][colIdx] = currTmaxAvgGrid[rowIdx][colIdx] / float64(yearCounter)
					currTminAvgGrid[rowIdx][colIdx] = currTminAvgGrid[rowIdx][colIdx] / float64(yearCounter)

					mMinMax.setMaxV(int64(currPotETGRid[rowIdx][colIdx]), "potET")
					mMinMax.setMaxV(int64(currPrecipSumGRid[rowIdx][colIdx]), "precipSum")
					mMinMax.setMaxV(int64(currTavgAvgGrid[rowIdx][colIdx]), "tavgAvg")
					mMinMax.setMaxV(int64(currTmaxAvgGrid[rowIdx][colIdx]), "tmaxAvg")
					mMinMax.setMaxV(int64(currTminAvgGrid[rowIdx][colIdx]), "tminAvg")

					mMinMax.setMinV(int64(currPotETGRid[rowIdx][colIdx]), "potET")
					mMinMax.setMinV(int64(currPrecipSumGRid[rowIdx][colIdx]), "precipSum")
					mMinMax.setMinV(int64(currTavgAvgGrid[rowIdx][colIdx]), "tavgAvg")
					mMinMax.setMinV(int64(currTmaxAvgGrid[rowIdx][colIdx]), "tmaxAvg")
					mMinMax.setMinV(int64(currTminAvgGrid[rowIdx][colIdx]), "tminAvg")
				}
			}
		}
	}
	writeGrid := func(name refGridName, grid [][]float64) {
		// save new grid
		outFileName := filepath.Join(outFolder, fmt.Sprintf(outfileTemplate[name], crop[setupId], scenario[setupId], co2[setupId], imageYear, imageYearIndex))
		fout := writeAGridHeader(outFileName, header)
		writeFloatRows(fout, grid)
		fout.Close()
		fileNameChan <- outFileName
	}
	writeGrid(yields, currentYearGrid)

	if withClimate {
		writeGrid(potET, currPotETGRid)
		writeGrid(precipSum, currPrecipSumGRid)
		writeGrid(tavgAvg, currTavgAvgGrid)
		writeGrid(tmaxAvg, currTmaxAvgGrid)
		writeGrid(tminAvg, currTminAvgGrid)
	}

	// save diff grid
	// if refGrid, ok := refGrids[yields]; ok {
	// 	diffgrid := createDiff(refGrid, currentYearGrid, nodata)

	// 	outDiffFileName := filepath.Join(outFolder, fmt.Sprintf(outDiffFileTemplate, crop[setupId], scenario[setupId], co2[setupId], imageYear, imageYearIndex))
	// 	fout := writeAGridHeader(outDiffFileName, header)
	// 	writeIntRows(fout, diffgrid)
	// 	fout.Close()
	// 	fileNameChan <- outDiffFileName
	// } else {
	// 	refGrids[yields] = currentYearGrid
	// }

	writeDiffGrid := func(name refGridName, grid [][]float64) {
		if refGrid, ok := refGrids[name]; ok {
			diffgrid := createDiff(refGrid, grid, nodata)

			outDiffFileName := filepath.Join(outFolder, fmt.Sprintf(outDiffFileTemplate[name], crop[setupId], scenario[setupId], co2[setupId], imageYear, imageYearIndex))
			fout := writeAGridHeader(outDiffFileName, header)
			writeIntRows(fout, diffgrid)
			fout.Close()
			fileNameChan <- outDiffFileName
		} else {
			refGrids[name] = grid
		}
	}
	writeDiffGrid(yields, currentYearGrid)

	if withClimate {
		writeDiffGrid(potET, currPotETGRid)
		writeDiffGrid(precipSum, currPrecipSumGRid)
		writeDiffGrid(tavgAvg, currTavgAvgGrid)
		writeDiffGrid(tmaxAvg, currTmaxAvgGrid)
		writeDiffGrid(tminAvg, currTminAvgGrid)
	}
	// save std grid
	outstdFileName := filepath.Join(outFolder, fmt.Sprintf(outStdfileTemplate, crop[setupId], scenario[setupId], co2[setupId], imageYear, imageYearIndex))
	foutStd := writeAGridHeader(outstdFileName, header)
	writeFloatRows(foutStd, stdeviationGrid)
	foutStd.Close()
	fileNameChan <- outstdFileName

	if outC != nil {
		outC <- mMinMax
	}

	return mMinMax
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

	potET     int
	precipSum int
	tavgAvg   int
	tmaxAvg   int
	tminAvg   int
}

func newMinMax() MinMax {
	return MinMax{
		minYield:  -1,
		maxYield:  -1,
		minStd:    -1,
		maxStd:    -1,
		potET:     -1,
		precipSum: -1,
		tavgAvg:   -1,
		tmaxAvg:   -1,
		tminAvg:   -1,
	}
}

func (m *MinMax) setMinV(min int64, fieldName string) {
	v := reflect.ValueOf(m).Elem()
	f := v.FieldByName(fieldName)
	curValue := f.Int()
	if curValue > (min) || curValue < 0 {
		f.SetInt((min))
	}
}
func (m *MinMax) setMaxV(max int64, fieldName string) {
	v := reflect.ValueOf(m).Elem()
	f := v.FieldByName(fieldName)
	curValue := f.Int()
	if curValue < (max) {
		f.SetInt((max))
	}
}

// func (m *MinMax) setMin(min int) {
// 	if m.minYield > min || m.minYield < 0 {
// 		m.minYield = min
// 	}
// }
// func (m *MinMax) setMax(max int) {
// 	if m.maxYield < max || m.maxYield < 0 {
// 		m.maxYield = max
// 	}
// }
// func (m *MinMax) setStdMin(min int) {
// 	if m.minStd > min || m.minStd < 0 {
// 		m.minStd = min
// 	}
// }
// func (m *MinMax) setStdMax(max int) {
// 	if m.maxStd < max || m.maxStd < 0 {
// 		m.maxStd = max
// 	}
// }
