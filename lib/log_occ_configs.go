package lib

import (
	"encoding/json"
	"fmt"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
	"io/ioutil"
	"regexp"
	"strings"
)

func extractValuesFromFile(file string) (map[string]string, error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	values := make(map[string]string)
	//get data from hera.txt and occ.def (config.go fetches everything from opscfg etc places and populates in hera.txt
	//hera.txt is source of truth
	switch file {
	case "occ.def", "hera.txt":
		re := regexp.MustCompile(`\b(\w+)\s*=\s*([^#\n]+)`)
		matches := re.FindAllStringSubmatch(string(content), -1)
		for _, match := range matches {
			values[match[1]] = strings.TrimSpace(match[2])
		}
		//case "config.go":
		//	re := regexp.MustCompile(`cdb\.GetOrDefaultString\("([^"]+)",\s*"([^"]+)"\)`)
		//	matches := re.FindAllStringSubmatch(string(content), -1)
		//	for _, match := range matches {
		//		values[match[1]] = strings.TrimSpace(match[2])
		//	}
	}

	return values, nil
}

func LogOccConfigs() error {
	//CAL events for OCC configs here
	whiteListConfigs := map[string][]string{
		"BACKLOG":  {"backlog_pct", "request_backlog_timeout"},
		"SHARDING": {"enable_sharding", "enable_sql_rewrite", "sharding_algo", "sharding_cross_keys_err", "sharding_postfix", "use_shardmap", "num_shards", "shard_key_name", "shard_key_value_type_is_string", "max_scuttle", "scuttle_col_name", "enable_whitelist_test", "whitelist_children", "sharding_cfg_reload_interval", "cfg_from_tns_override_num_shards"},
	}

	//dir, _ := os.Getwd()
	//fmt.Println("pwd: ", dir)

	//Set the file search path to the current working directory
	//err := os.Chdir(dir + "/lib")
	//if err != nil {
	//	fmt.Println("Error:", err)
	//	return nil
	//}

	// location of files to search values of the configs from
	files := []string{"occ.def", "hera.txt"}
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
	}
	for feature, configs := range collectedValues {
		configsMarshal, _ := json.Marshal(configs)
		configsMarshalStr := string(configsMarshal)

		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "list of configs within the feature:", feature, ":", configsMarshalStr)
		}

		evt := cal.NewCalEvent("OCC_CONFIG", fmt.Sprintf(feature), cal.TransOK, configsMarshalStr, "")
		evt.Completed()
	}

	return nil
}
