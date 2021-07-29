package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/babkamen/urlshort"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var (
	jsonFilepath = flag.String("paths-json", "", "filepath to json with paths map")
	yamlFilepath = flag.String("paths-yaml", "", "filepath to yaml with paths map")
	postgresUrl  = flag.String("postgres-url", "", "url to postgres server")
)

func main() {

	flagsIsEmpty := *jsonFilepath == "" && *yamlFilepath == "" && *postgresUrl == ""
	if contains(os.Args, "--help") || flagsIsEmpty {
		printHelp()
	}
	flag.Parse()
	redirects := mergeMaps(
		createRedirectsFromJson,
		createRedirectsFromYaml,
		createRedirectsFromPostgres)
	fmt.Printf("Redirects %v", redirects)

	mux := defaultMux()
	handler := urlshort.MapHandler(redirects, mux)
	fmt.Println("Starting the server on :8080")
	err := http.ListenAndServe(":8080", handler)
	logFatal("Error while starting server", err)
}

func mergeMaps(functions ...redirectCreator) map[string]string {
	result := make(map[string]string)
	for _, f := range functions {
		m := f()
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

func createRedirectsFromPostgres() map[string]string {
	connStr := "postgres://root:password@localhost/urlshort?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	logFatal("Error while connecting to postgres", err)
	defer close("db", db)

	rows, err := db.Query("SELECT path, url FROM redirects")
	logFatal("Error while connecting to postgres", err)
	defer close("rows", rows)

	redirects := make(map[string]string)

	for rows.Next() {
		var (
			path     string
			redirect string
		)
		if err := rows.Scan(&path, &redirect); err != nil {
			log.Fatal(err)
		}
		redirects[path] = redirect

	}
	logFatal("Error in rows", rows.Err())

	return redirects
}

func close(name string, c io.Closer) {
	logFatal("Error while closing "+name, c.Close())
}

func createRedirectsFromYaml() map[string]string {
	if *yamlFilepath == "" {
		return make(map[string]string)
	}

	yamlData, err := ioutil.ReadFile(*yamlFilepath)
	logFatal("Error while reading yaml file", err)

	var redirects []redirect
	err = yaml.Unmarshal(yamlData, &redirects)
	logFatal("Error while converting yaml", err)
	return convertToMap(redirects)
}

func createRedirectsFromJson() map[string]string {
	var result map[string]string

	if *jsonFilepath == "" {
		return result
	}

	jsonData, err := ioutil.ReadFile(*jsonFilepath)
	logFatal("Error while reading json file", err)
	err = json.Unmarshal(jsonData, &result)
	logFatal("Error while converting json", err)

	return result
}

func defaultMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", health)
	return mux
}

func health(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintln(w, "UP")
	logFatal("Error while create a sample handler", err)
}

func printHelp() {
	_, err := fmt.Fprintf(os.Stderr, "usage: %s [flags] <paths...>\n", os.Args[0])
	logFatal("Error while printing usage", err)
	flag.PrintDefaults()
	os.Exit(0)
}

func contains(str []string, searchterm string) bool {
	for _, s := range str {
		if s == searchterm {
			return true
		}
	}
	return false
}

func logFatal(message string, err error) {
	if err != nil {
		log.Fatal(message, " ", err)
	}
}

func convertToMap(redirects []redirect) map[string]string {
	result := make(map[string]string, len(redirects))
	for _, v := range redirects {
		result[v.Path] = v.Url
	}
	return result
}

type redirect struct {
	Path string `yaml:"path"`
	Url  string `yaml:"url"`
}

type redirectCreator func() map[string]string
