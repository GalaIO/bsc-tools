package main

import (
	"encoding/json"
	"fmt"
	"github.com/GalaIO/bsc-tools/utils"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	PreRewardFileName = "ana.pre.reward.log"
	RewardFileName    = "ana.reward.log"
	Reward20FileName  = "ana.reward20.log"
	EmptyBlkFileName  = "ana.emptyBlk.log"
	SlashFileName     = "ana.slash.log"
	BlockFileName     = "ana.block.log"
)

var (
	assetDir     string
	outputDir    string
	Int0         = new(big.Int).SetUint64(0)
	MinBNB, _    = new(big.Int).SetString("1000000000000000", 10)
	FilterToTime = []string{
		"2022-10-12",
	}
	ValNameMap map[string]*ValidatorInfo
)

type ValidatorInfo struct {
	Url     string `json:"url"`
	Addr    string `json:"addr"`
	NameTag string `json:"nameTag"`
}

type FilterLogItem struct {
	subStr    string
	nextLines []string
}

func init() {
	root, err := os.Getwd()
	utils.PanicErr(err)
	assetDir = filepath.Join(root, "reward", "asset")
	outputDir = filepath.Join(root, "reward", "output")
	ValNameMapStr := utils.ReadString(filepath.Join(assetDir, "validator_mapping.json"))
	err = json.Unmarshal([]byte(ValNameMapStr), &ValNameMap)
	utils.PanicErr(err)
}

func main() {
	if len(os.Args) < 2 {
		panic("please input reward.log path params")
	}

	rewardLog := os.Args[1]
	parseLogs(rewardLog)
	outputRewardChart(RewardFileName, "Reward")
	fmt.Printf("\n\n===================\n\n")
	outputRewardChart(Reward20FileName, "Reward2.0")
	outputRewardCompareChart([]string{RewardFileName, Reward20FileName}, []string{"Reward", "Reward 2.0"}, "Reward comparison")
	outputEmptyBlkChart(EmptyBlkFileName, "EmptyBlock")
	outputSlashChart(SlashFileName, "Slash")
	outputBlkAndTxChart(BlockFileName, "BlockCount", 0)
	outputBlkAndTxChart(BlockFileName, "TxCount", 1)
}

func mid(v []float64) float64 {
	sort.Float64s(v)
	return v[len(v)/2]
}

func min(v []float64) float64 {
	var res float64 = v[0]
	for i := 1; i < len(v); i++ {
		if v[i] < res {
			res = v[i]
		}
	}
	return res
}

func max(v []float64) float64 {
	var res float64 = v[0]
	for i := 1; i < len(v); i++ {
		if v[i] > res {
			res = v[i]
		}
	}
	return res
}

func mean(v []float64) float64 {
	var res float64 = 0
	var n int = len(v)
	for i := 0; i < n; i++ {
		res += v[i]
	}
	return res / float64(n)
}

func variance(v []float64) float64 {
	var res float64 = 0
	var m = mean(v)
	var n int = len(v)
	for i := 0; i < n; i++ {
		res += (v[i] - m) * (v[i] - m)
	}
	return res / float64(n-1)
}

func std(v []float64) float64 {
	return math.Sqrt(variance(v))
}

func outputRewardCompareChart(srcFiles []string, renames []string, finalName string) {
	finalXAxisData := []string{}
	finalYAxisData := []*Series{}
	finalLegend := []string{}

	filterNames := []string{
		"SystemReward",
	}

	for i := range srcFiles {
		srcFile := srcFiles[i]
		rename := renames[i]

		xAxisData, yAxisData, _ := parseReward(srcFile)
		finalXAxisData = []string{}

		yAvg := &Series{
			Name:  rename + "-avg",
			LType: "line",
			Stack: "Total",
			Data:  []interface{}{},
		}
		yMax := &Series{
			Name:  rename + "-max",
			LType: "line",
			Stack: "Total",
			Data:  []interface{}{},
		}
		yMin := &Series{
			Name:  rename + "-min",
			LType: "line",
			Stack: "Total",
			Data:  []interface{}{},
		}
		yMid := &Series{
			Name:  rename + "-mid",
			LType: "line",
			Stack: "Total",
			Data:  []interface{}{},
		}
		yVar := &Series{
			Name:  rename + "-variance",
			LType: "line",
			Stack: "Total",
			Data:  []interface{}{},
		}
		yBias := &Series{
			Name:  rename + "-(max-mid)",
			LType: "line",
			Stack: "Total",
			Data:  []interface{}{},
		}
		finalLegend = append(finalLegend, yAvg.Name, yMax.Name, yMin.Name, yMid.Name, yVar.Name, yBias.Name)

		for j := range xAxisData {
			tmp := []float64{}
			for k := range yAxisData {
				if utils.FindMatch(yAxisData[k].Name, filterNames) >= 0 {
					continue
				}
				d := yAxisData[k].Data[j]
				switch d.(type) {
				case int64:
					num := d.(int64)
					if num > 0 {
						tmp = append(tmp, float64(num))
					}
				case string:
					//tmp = append(tmp, float64(0))
				case float64:
					num := d.(float64)
					if num > 0 {
						tmp = append(tmp, num)
					}
				default:
					panic("not support type")
				}
			}

			// if too small, kust skip
			if len(tmp) < 10 {
				continue
			}
			finalXAxisData = append(finalXAxisData, xAxisData[j])
			tmpMax := max(tmp)
			yMax.Data = append(yMax.Data, tmpMax)
			yMin.Data = append(yMin.Data, min(tmp))
			yAvg.Data = append(yAvg.Data, mean(tmp))
			yVar.Data = append(yVar.Data, std(tmp))
			tmpMid := mid(tmp)
			yMid.Data = append(yMid.Data, tmpMid)
			yBias.Data = append(yBias.Data, tmpMax-tmpMid)
		}

		finalYAxisData = append(finalYAxisData, yMax, yMin, yAvg, yMid, yVar, yBias)
	}

	// out put chart
	legendBytes, err := json.Marshal(finalLegend)
	utils.PanicErr(err)
	legendStr := string(legendBytes)
	fmt.Println("legendStr", legendStr)

	xAxisDataBytes, err := json.Marshal(finalXAxisData)
	utils.PanicErr(err)

	yAxisData := []*SeriesView{}
	for _, item := range finalYAxisData {

		itemBytes, _ := json.Marshal(item.Data)
		v := &SeriesView{
			Name:  item.Name,
			LType: item.LType,
			Stack: item.Stack,
			Data:  string(itemBytes),
		}
		yAxisData = append(yAxisData, v)
	}
	yAxisDataBytes, err := json.MarshalIndent(yAxisData, "", "    ")
	utils.PanicErr(err)
	yAxisDataStr := cleanStr(string(yAxisDataBytes))
	fmt.Println(yAxisDataStr)

	tempStr := utils.ReadString(filepath.Join(assetDir, "line_chart_template.html"))
	tempStr = strings.ReplaceAll(tempStr, "{{title}}", finalName)
	tempStr = strings.ReplaceAll(tempStr, "{{subTitle}}", "")
	tempStr = strings.ReplaceAll(tempStr, "{{unit}}", "BNB")
	tempStr = strings.ReplaceAll(tempStr, "{{xName}}", "time")
	tempStr = strings.ReplaceAll(tempStr, "{{yName}}", "reward")
	tempStr = strings.ReplaceAll(tempStr, "{{legend}}", legendStr)
	tempStr = strings.ReplaceAll(tempStr, "{{xAxisData}}", string(xAxisDataBytes))
	tempStr = strings.ReplaceAll(tempStr, "{{series}}", yAxisDataStr)
	utils.WriteString(filepath.Join(outputDir, finalName+"_line_chart.html"), tempStr)
}

type Series struct {
	Name  string        `json:"name"`
	LType string        `json:"type"`
	Stack string        `json:"stack"`
	Data  []interface{} `json:"data"`
}

type SeriesView struct {
	Name  string `json:"name"`
	LType string `json:"type"`
	Stack string `json:"-"`
	Data  string `json:"data"`
}

type PieItem struct {
	Name  string `json:"name"`
	Value uint64 `json:"value"`
}

func outputEmptyBlkChart(fileName, rename string) {
	lines := utils.ReadLines(filepath.Join(outputDir, fileName))

	emptyCountMap := map[string]*PieItem{}
	blks := []string{}
	count := 0

	for _, line := range lines {
		index := strings.Index(line, "[Find empty block]")
		if index >= 0 {
			items := strings.Split(line[index:], " ")
			blks = append(blks, items[3])
			name := items[4]
			if vn, ok := ValNameMap[name]; ok {
				name = vn.NameTag
			}
			if _, ok := emptyCountMap[name]; !ok {
				emptyCountMap[name] = &PieItem{
					Name:  name,
					Value: 0,
				}
			}

			emptyCountMap[name].Value++
			count++
		}
	}

	total := utils.ParseInt64(blks[len(blks)-1]) - utils.ParseInt64(blks[0])
	subTitle := fmt.Sprintf("Total Block: %v, Empty count: %v, Ratio: %.4f%%", total, count, float64(1.0*count)/float64(total)*100)

	items := []*PieItem{}
	for _, item := range emptyCountMap {
		items = append(items, item)
	}

	emptyBytes, err := json.MarshalIndent(items, "", "    ")
	utils.PanicErr(err)
	emptyStr := string(emptyBytes)

	tempStr := utils.ReadString(filepath.Join(assetDir, "pie_chart_template.html"))
	tempStr = strings.ReplaceAll(tempStr, "{{title}}", rename)
	tempStr = strings.ReplaceAll(tempStr, "{{subTitle}}", subTitle)
	tempStr = strings.ReplaceAll(tempStr, "{{data}}", emptyStr)
	utils.WriteString(filepath.Join(outputDir, rename+"_pie_chart.html"), tempStr)
}

func outputSlashChart(fileName, rename string) {
	lines := utils.ReadLines(filepath.Join(outputDir, fileName))

	emptyCountMap := map[string]*PieItem{}
	blks := []string{}
	count := 0

	for _, line := range lines {
		index := strings.Index(line, "[ValidatorMisdemeanor]")
		if index < 0 {
			index = strings.Index(line, "[ValidatorFelony]")
		}
		if index >= 0 {
			items := strings.Split(line[index:], " ")
			blks = append(blks, items[2])
			name := items[5]
			if vn, ok := ValNameMap[name]; ok {
				name = vn.NameTag
			}
			if _, ok := emptyCountMap[name]; !ok {
				emptyCountMap[name] = &PieItem{
					Name:  name,
					Value: 0,
				}
			}

			emptyCountMap[name].Value++
			count++
		}
	}

	total := utils.ParseInt64(blks[len(blks)-1]) - utils.ParseInt64(blks[0])
	subTitle := fmt.Sprintf("Total Block: %v, Slash count: %v, Ratio: %.4f%%", total, count, float64(1.0*count)/float64(total)*100)

	items := []*PieItem{}
	for _, item := range emptyCountMap {
		items = append(items, item)
	}

	emptyBytes, err := json.MarshalIndent(items, "", "    ")
	utils.PanicErr(err)
	emptyStr := string(emptyBytes)

	tempStr := utils.ReadString(filepath.Join(assetDir, "pie_chart_template.html"))
	tempStr = strings.ReplaceAll(tempStr, "{{title}}", rename)
	tempStr = strings.ReplaceAll(tempStr, "{{subTitle}}", subTitle)
	tempStr = strings.ReplaceAll(tempStr, "{{data}}", emptyStr)
	utils.WriteString(filepath.Join(outputDir, rename+"_pie_chart.html"), tempStr)
}

func outputBlkAndTxChart(fileName, rename string, cate int) {
	xAxisData, seriesMap, valAddrs := parseBlockOrTx(fileName, cate)

	valBytes, err := json.Marshal(valAddrs)
	utils.PanicErr(err)
	valStr := string(valBytes)
	fmt.Println("valAddrs", valStr)

	xAxisDataBytes, err := json.Marshal(xAxisData)
	utils.PanicErr(err)

	yAxisData := []*SeriesView{}
	for _, item := range seriesMap {

		itemBytes, _ := json.Marshal(item.Data)
		v := &SeriesView{
			Name:  item.Name,
			LType: item.LType,
			Stack: item.Stack,
			Data:  string(itemBytes),
		}
		yAxisData = append(yAxisData, v)
	}
	yAxisDataBytes, err := json.MarshalIndent(yAxisData, "", "    ")
	utils.PanicErr(err)
	yAxisDataStr := cleanStr(string(yAxisDataBytes))
	fmt.Println(yAxisDataStr)

	tempStr := utils.ReadString(filepath.Join(assetDir, "line_chart_template.html"))
	tempStr = strings.ReplaceAll(tempStr, "{{title}}", rename)
	tempStr = strings.ReplaceAll(tempStr, "{{subTitle}}", fileName)
	tempStr = strings.ReplaceAll(tempStr, "{{unit}}", "")
	tempStr = strings.ReplaceAll(tempStr, "{{xName}}", "time")
	tempStr = strings.ReplaceAll(tempStr, "{{yName}}", "")
	tempStr = strings.ReplaceAll(tempStr, "{{legend}}", valStr)
	tempStr = strings.ReplaceAll(tempStr, "{{xAxisData}}", string(xAxisDataBytes))
	tempStr = strings.ReplaceAll(tempStr, "{{series}}", yAxisDataStr)
	utils.WriteString(filepath.Join(outputDir, rename+"_line_chart.html"), tempStr)
}

func outputRewardChart(fileName string, rename string) {
	xAxisData, seriesMap, valAddrs := parseReward(fileName)

	valBytes, err := json.Marshal(valAddrs)
	utils.PanicErr(err)
	valStr := string(valBytes)
	fmt.Println("valAddrs", valStr)

	xAxisDataBytes, err := json.Marshal(xAxisData)
	utils.PanicErr(err)

	yAxisData := []*SeriesView{}
	for _, item := range seriesMap {

		itemBytes, _ := json.Marshal(item.Data)
		v := &SeriesView{
			Name:  item.Name,
			LType: item.LType,
			Stack: item.Stack,
			Data:  string(itemBytes),
		}
		yAxisData = append(yAxisData, v)
	}
	yAxisDataBytes, err := json.MarshalIndent(yAxisData, "", "    ")
	utils.PanicErr(err)
	yAxisDataStr := cleanStr(string(yAxisDataBytes))
	fmt.Println(yAxisDataStr)

	tempStr := utils.ReadString(filepath.Join(assetDir, "line_chart_template.html"))
	tempStr = strings.ReplaceAll(tempStr, "{{title}}", rename)
	tempStr = strings.ReplaceAll(tempStr, "{{unit}}", "BNB")
	tempStr = strings.ReplaceAll(tempStr, "{{xName}}", "time")
	tempStr = strings.ReplaceAll(tempStr, "{{yName}}", "reward")
	tempStr = strings.ReplaceAll(tempStr, "{{subTitle}}", fileName)
	tempStr = strings.ReplaceAll(tempStr, "{{legend}}", valStr)
	tempStr = strings.ReplaceAll(tempStr, "{{xAxisData}}", string(xAxisDataBytes))
	tempStr = strings.ReplaceAll(tempStr, "{{series}}", yAxisDataStr)
	utils.WriteString(filepath.Join(outputDir, rename+"_line_chart.html"), tempStr)
}

func parseReward(fileName string) ([]string, map[string]*Series, []string) {
	rewardLines := utils.ReadLines(filepath.Join(outputDir, fileName))

	xAxisData := []string{}
	seriesMap := map[string]*Series{}
	index := -1

	for i := 0; i < len(rewardLines); i++ {
		line := rewardLines[i]
		if strings.Contains(line, "[Update validator set]") {
			times := strings.Split(line, " ")
			sec, err := strconv.ParseInt(times[len(times)-1], 10, 64)
			utils.PanicErr(err)
			toTime := time.Unix(sec, 0)
			fmt.Println("Update validator set in", toTime)
			to := toTime.Format("2006-01-02")
			if len(xAxisData) > 0 && xAxisData[len(xAxisData)-1] == to {
				continue
			}

			// filter
			if utils.FindMatch(to, FilterToTime) >= 0 {
				continue
			}

			index++
			xAxisData = append(xAxisData, to)

			// parse later lines
			for i+1 < len(rewardLines) {
				nextLine := rewardLines[i+1]
				if strings.Contains(nextLine, "[Reward parsed]") ||
					strings.Contains(nextLine, "[Reward 2.0 review]") ||
					strings.Contains(nextLine, "[Reward 1.0 review]") {
					// pass
					i++
				} else if strings.Contains(nextLine, "[Validator]") {
					items := strings.Split(nextLine, " ")
					name := items[1]
					if vn, ok := ValNameMap[name]; ok {
						name = vn.NameTag
					}
					if _, ok := seriesMap[name]; !ok {
						seriesMap[name] = &Series{
							Name:  name,
							LType: "line",
							Stack: "Total",
							Data:  []interface{}{},
						}
					}

					// fill empty
					bias := index - len(seriesMap[name].Data)
					for l := 0; l < bias; l++ {
						seriesMap[name].Data = append(seriesMap[name].Data, "-")
					}
					num, succ := new(big.Int).SetString(items[2], 10)
					if !succ {
						panic("wrong validator reward")
					}
					if num.Cmp(Int0) == 0 {
						seriesMap[name].Data = append(seriesMap[name].Data, "-")
					} else {
						num.Div(num, MinBNB)
						numf := float64(1.0*num.Int64()) / 1000.0
						seriesMap[name].Data = append(seriesMap[name].Data, numf)
					}
					i++
				} else if strings.Contains(nextLine, "[System reward]") {
					items := strings.Split(nextLine, " ")
					name := "SystemReward"
					if vn, ok := ValNameMap[name]; ok {
						name = vn.NameTag
					}
					if _, ok := seriesMap[name]; !ok {
						seriesMap[name] = &Series{
							Name:  name,
							LType: "line",
							Stack: "Total",
							Data:  []interface{}{},
						}
					}

					// fill empty
					bias := index - len(seriesMap[name].Data)
					for l := 0; l < bias; l++ {
						seriesMap[name].Data = append(seriesMap[name].Data, "-")
					}
					num, succ := new(big.Int).SetString(items[2], 10)
					if !succ {
						panic("wrong system reward")
					}
					numf := 1.0 * num.Div(num, MinBNB).Int64() / 1000
					seriesMap[name].Data = append(seriesMap[name].Data, numf)
					i++
				} else {
					break
				}
			}
		}
	}

	// fill empty
	legends := make([]string, 0, len(seriesMap))
	for s := range seriesMap {
		bias := len(xAxisData) - len(seriesMap[s].Data)
		for i := 0; i < bias; i++ {
			seriesMap[s].Data = append(seriesMap[s].Data, "-")
		}
		legends = append(legends, s)
	}

	return xAxisData, seriesMap, legends
}

func parseBlockOrTx(fileName string, cate int) ([]string, map[string]*Series, []string) {
	rewardLines := utils.ReadLines(filepath.Join(outputDir, fileName))

	xAxisData := []string{}
	seriesMap := map[string]*Series{}
	index := -1

	blockFilter := map[string]bool{}

	for i := 0; i < len(rewardLines); i++ {
		line := rewardLines[i]
		if strings.Contains(line, "[Update validator set]") {
			times := strings.Split(line, " ")
			sec, err := strconv.ParseInt(times[len(times)-1], 10, 64)
			utils.PanicErr(err)
			toTime := time.Unix(sec, 0)
			fmt.Println("Update validator set in", toTime)
			to := toTime.Format("2006-01-02")
			if len(xAxisData) > 0 && xAxisData[len(xAxisData)-1] == to {
				continue
			}

			// filter
			if utils.FindMatch(to, FilterToTime) >= 0 {
				continue
			}

			index++
			xAxisData = append(xAxisData, to)

			// parse later lines
			for i+1 < len(rewardLines) {
				nextLine := rewardLines[i+1]
				if bi := strings.Index(nextLine, "[Block Reward]"); bi >= 0 {
					items := strings.Split(nextLine[bi:], " ")
					blockNum := items[3]
					name := items[6]

					// filter same block
					if _, ok := blockFilter[blockNum]; ok {
						continue
					}
					blockFilter[blockNum] = true
					if vn, ok := ValNameMap[name]; ok {
						name = vn.NameTag
					}
					if _, ok := seriesMap[name]; !ok {
						seriesMap[name] = &Series{
							Name:  name,
							LType: "line",
							Stack: "Total",
							Data:  []interface{}{},
						}
					}

					// fill empty
					bias := index - len(seriesMap[name].Data)
					for l := 0; l < bias; l++ {
						seriesMap[name].Data = append(seriesMap[name].Data, "-")
					}
					if len(seriesMap[name].Data) < index+1 {
						seriesMap[name].Data = append(seriesMap[name].Data, int64(0))
					}

					// parse value
					switch cate {
					// block
					case 0:
						seriesMap[name].Data[index] = seriesMap[name].Data[index].(int64) + 1
					// tx
					case 1:
						seriesMap[name].Data[index] = seriesMap[name].Data[index].(int64) + utils.ParseInt64(items[12])
					}
					i++
				} else {
					break
				}
			}
		}
	}

	// fill empty
	legends := make([]string, 0, len(seriesMap))
	for s := range seriesMap {
		bias := len(xAxisData) - len(seriesMap[s].Data)
		for i := 0; i < bias; i++ {
			seriesMap[s].Data = append(seriesMap[s].Data, "-")
		}
		legends = append(legends, s)
	}

	return xAxisData, seriesMap, legends
}

func cleanStr(s string) string {
	s = strings.ReplaceAll(s, "\\\"-\\\"", "\"-\"")
	s = strings.ReplaceAll(s, "\\\"", "\"")
	s = strings.ReplaceAll(s, "\\\"", "\"")
	s = strings.ReplaceAll(s, "\"[", "[")
	s = strings.ReplaceAll(s, "]\"", "]")
	return s
}

func parseLogs(name string) {
	tmpLines := utils.ReadLines(name)

	preRewardLines := filterLogs(tmpLines, []FilterLogItem{
		{"[Update validator set]", nil},
		{"[Reward parsed]", nil},
		{"[Reward 1.0 review]", nil},
		{"[Reward 2.0 review]", nil},
		{"[Validator]", nil},
		{"[System reward]", nil},
		{"[Block Reward]", nil},
	})
	utils.WriteLines(filepath.Join(outputDir, PreRewardFileName), preRewardLines)

	rewardLines := filterLogs(preRewardLines, []FilterLogItem{
		{"[Update validator set]", nil},
		{"[Reward parsed]", []string{"[Validator]", "[System reward]"}},
	})
	utils.WriteLines(filepath.Join(outputDir, RewardFileName), rewardLines)

	reward20Lines := filterLogs(preRewardLines, []FilterLogItem{
		{"[Update validator set]", nil},
		{"[Reward 2.0 review]", []string{"[Validator]", "[System reward]"}},
	})
	utils.WriteLines(filepath.Join(outputDir, Reward20FileName), reward20Lines)

	emptyBlkLines := filterLogs(tmpLines, []FilterLogItem{
		{"[Find empty block]", nil},
	})
	utils.WriteLines(filepath.Join(outputDir, EmptyBlkFileName), emptyBlkLines)

	slashLines := filterLogs(tmpLines, []FilterLogItem{
		{"[ValidatorMisdemeanor]", nil},
		{"[ValidatorFelony]", nil},
	})
	utils.WriteLines(filepath.Join(outputDir, SlashFileName), slashLines)

	blkLines := filterLogs(tmpLines, []FilterLogItem{
		{"[Update validator set]", nil},
		{"[Block Reward]", nil},
	})
	utils.WriteLines(filepath.Join(outputDir, BlockFileName), blkLines)
}

func filterLogs(lines []string, items []FilterLogItem) []string {

	var ret []string
	for i := 0; i < len(lines); i++ {
		index := findMatchItem(lines[i], items)
		if index < 0 {
			continue
		}

		ret = append(ret, lines[i])
		for i+1 < len(lines) {
			if utils.FindMatch(lines[i+1], items[index].nextLines) < 0 {
				break
			}
			ret = append(ret, lines[i+1])
			i++
		}
	}

	return ret
}

func findMatchItem(s string, subs []FilterLogItem) int {
	for i := range subs {
		if strings.Contains(s, subs[i].subStr) {
			return i
		}
	}

	return -1
}
