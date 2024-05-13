package lib

import (
	"encoding/json"
	"fmt"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

func extractValuesFromFile(file string) (map[string]string, error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	values := make(map[string]string)

	switch file {
	case "occ.def", "occ-account.def":
		re := regexp.MustCompile(`\b(\w+)\s*=\s*([^#\n]+)`)
		matches := re.FindAllStringSubmatch(string(content), -1)
		for _, match := range matches {
			values[match[1]] = strings.TrimSpace(match[2])
		}
	case "config.go":
		re := regexp.MustCompile(`cdb\.GetOrDefaultString\("([^"]+)",\s*"([^"]+)"\)`)
		matches := re.FindAllStringSubmatch(string(content), -1)
		for _, match := range matches {
			values[match[1]] = strings.TrimSpace(match[2])
		}
	}

	return values, nil
}

func LogOccConfigs() error {
	//CAL events for OCC configs here
	whiteListConfigs := map[string][]string{
		"BACKLOG":  {"backlog_pct", "request_backlog_timeout"},
		"SHARDING": {"enable_sharding", "enable_sql_rewrite", "sharding_algo", "sharding_cross_keys_err", "sharding_postfix", "use_shardmap", "num_shards", "shard_key_name", "shard_key_value_type_is_string", "max_scuttle", "scuttle_col_name", "enable_whitelist_test", "whitelist_children", "sharding_cfg_reload_interval", "cfg_from_tns_override_num_shards"},
	}

	//debug
	dir, _ := os.Getwd()
	fmt.Println("pwd: ", dir)

	//Set the file search path to the current working directory
	//err := os.Chdir(dir + "/lib")
	//if err != nil {
	//	fmt.Println("Error:", err)
	//	return nil
	//}

	// location of files to search values of the configs from
	files := []string{"occ.def", "occ-account.def", "config.go"}
	// fetch values of all whiteListConfigs
	collectedValues := make(map[string]map[string]string)

	for _, file := range files {
		values, err := extractValuesFromFile(file)
		if err != nil {
			fmt.Printf("Error reading file %s: %v\n", file, err)
			continue
		}

		// Compare collected values with configList
		for feature, configs := range whiteListConfigs {
			for _, config := range configs {
				if value, ok := values[config]; ok {
					if _, found := collectedValues[feature]; !found {
						collectedValues[feature] = make(map[string]string)
					}
					collectedValues[feature][config] = value
				}
			}
		}
		//print collected values
	}
	for feature, configs := range collectedValues {
		//fmt.Printf("%s:\n", feature)

		//for config, value := range configs {
		//	fmt.Printf("\t%s: %s\n", config, value)
		//}
		configsMarshal, _ := json.Marshal(configs)
		configsMarshalStr := string(configsMarshal)

		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "list of configs within the feature:", feature, ":", configsMarshalStr)
		}

		//fmt.Printf("configs in feature: %s \n", configsMarshalStr)

		evt := cal.NewCalEvent("OCC_CONFIG", fmt.Sprintf("list of configs within the feature: %v", feature), cal.TransOK, configsMarshalStr)
		evt.Completed()
	}

	////blacklist_configs : = {}
	//for key, value := range whiteListConfigs {
	//	evt := cal.NewCalEvent("OCC_CONFIG", fmt.Sprintf("list of configs within the feature: %v", key), cal.TransOK, strings.Join(value, ", "))
	//	evt.Completed()
	//}

	return nil
}
