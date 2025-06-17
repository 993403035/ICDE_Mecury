package LoadAware

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

func writeCommunitiesToCSV(community map[string]int) {
	file, _ := os.Create("Data.csv")
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	// Write header
	writer.Write([]string{"Addr", "ID"})
	// Write community data
	for v, ID := range community {
		writer.Write([]string{v, strconv.Itoa(ID)})
	}
	fmt.Println("Communities written to InitLouvain.csv")
}

//func writeCommunitiesToCSV(community map[string]int) {
//	// 打开文件进行写入
//	file, _ := os.Create("D:\\block-emulato_2025\\label.csv")
//	defer file.Close()
//
//	// 使用 fmt.Fprintf 将 labelMap 写入文件
//	for addr, label := range community {
//		_, err := fmt.Fprintf(file, "%s: %d\n", addr, label)
//		if err != nil {
//			fmt.Println("Failed to write to file:", err)
//			return
//		}
//	}
//
//	fmt.Println("Label map has been written to:", file)
//}

func WriteLabelMapToFile(labelMap map[string]int, filename string) {
	// 打开文件进行写入
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Failed to create file:", err)
		return
	}
	defer file.Close()

	// 使用 fmt.Fprintf 将 labelMap 写入文件
	for addr, label := range labelMap {
		_, err := fmt.Fprintf(file, "%s: %d\n", addr, label)
		if err != nil {
			fmt.Println("Failed to write to file:", err)
			return
		}
	}

	fmt.Println("Label map has been written to:", filename)
}
