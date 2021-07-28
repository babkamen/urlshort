package urlshort

import (
	"gopkg.in/yaml.v2"
	"net/http"
)

// MapHandler will return an http.HandlerFunc (which also
// implements http.Handler) that will attempt to map any
// paths (keys in the map) to their corresponding URL (values
// that each key in the map points to, in string format).
// If the path is not provided in the map, then the fallback
// http.Handler will be called instead.
func MapHandler(pathsToUrls map[string]string, fallback http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		newUrl, found := pathsToUrls[r.URL.String()]
		if found {
			http.Redirect(w, r, newUrl, http.StatusSeeOther)
		} else {
			fallback.ServeHTTP(w, r)
		}
	}
}

// YAMLHandler will parse the provided YAML and then return
// an http.HandlerFunc (which also implements http.Handler)
// that will attempt to map any paths to their corresponding
// URL. If the path is not provided in the YAML, then the
// fallback http.Handler will be called instead.
//
// YAML is expected to be in the format:
//
//     - path: /some-path
//       url: https://www.some-url.com/demo
//
// The only errors that can be returned all related to having
// invalid YAML data.
//
// See MapHandler to create a similar http.HandlerFunc via
// a mapping of paths to urls.
func YAMLHandler(yml []byte, fallback http.Handler) (http.HandlerFunc, error) {
	var redirects []redirect
	err := yaml.Unmarshal(yml, &redirects)
	if err != nil {
		return nil, err
	}
	return MapHandler(convertToMap(redirects), fallback), nil
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
