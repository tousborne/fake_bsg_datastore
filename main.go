package main

import "bytes"
import "compress/gzip"
import "encoding/base64"
import "encoding/json"
import "io/ioutil"
import "fmt"
import "net/http"

const MAXBYTES = 1000

func display(writer http.ResponseWriter, request *http.Request) {
	fmt.Printf("######\n")
	fmt.Printf("# %s request to %s\n", request.Method, request.URL)

	userAgent, ok := request.Header["User-Agent"]
	if ok {
		fmt.Printf("# from %s\n", userAgent)
	}

	contentType, ok := request.Header["Content-Type"]
	if ok {
		fmt.Printf("# %s\n", contentType)
	}

	contentLength, ok := request.Header["Content-Length"]
	if ok {
		fmt.Printf("# %s bytes\n", contentLength)
	}

	err := request.ParseForm()
	if err != nil {
		fmt.Printf("# form: %+v\n", request.Form)
	}

	err = request.ParseMultipartForm(50)
	if err == nil {
		if len(request.MultipartForm.File) != 0 {
			fmt.Printf("# multipart files:\n")
		}

		for file, handles := range request.MultipartForm.File {
			for _, handle := range handles {
				fmt.Printf("# %s: %d bytes\n", handle.Filename, handle.Size)

				reader, err := handle.Open()
				if err != nil {
					fmt.Printf("# Error opening file: %s\n", err)
					continue
				}

				data, err := ioutil.ReadAll(reader)
				if err != nil {
					fmt.Printf("# Error reading file: %s\n", err)
				}

				if file == "dataFile" {
					reader, err := gzip.NewReader(bytes.NewReader(data))
					if err != nil {
						fmt.Printf("# Error opening gzipped data: %s\n", err)
						continue
					}

					uncompressed, err := ioutil.ReadAll(reader)
					if err != nil {
						fmt.Printf("# Error reading gzipped data: %s\n", err)
						continue
					}

					fmt.Printf("# Decoded gzip data\n")

					if len(uncompressed) > MAXBYTES {
						fmt.Printf("# Note: cut output to %d bytes\n", MAXBYTES)
						uncompressed = uncompressed[0:MAXBYTES]
					}

					data = uncompressed
				}

				fmt.Printf("#\t%s:\n%s\n", file, data)
			}
		}

		if len(request.MultipartForm.Value) != 0 {
			fmt.Printf("# multpart vaues:\n")
		}

		for key, value := range request.MultipartForm.Value {
			var jsonValue []map[string]string

			for _, element := range value {
				var jsonData map[string]string

				err := json.Unmarshal([]byte(element), &jsonData)
				if err != nil {
					fmt.Printf("# Error decoding json: %s\n", err)
					continue
				}

				jsonValue = append(jsonValue, jsonData)
			}

			if key == "item" {
				for _, element := range jsonValue {
					encoded, exists := element["data"]

					if exists {
						decoded, err := base64.StdEncoding.DecodeString(encoded)
						if err != nil {
							fmt.Printf("# Error decoding base64 data: %s\n", err)
							continue
						}

						fmt.Printf("# Decoded base64 data\n")

						if len(decoded) > MAXBYTES {
							fmt.Printf("# Note: cut output to %d bytes\n", MAXBYTES)
							decoded = decoded[0:MAXBYTES]
						}

						element["data"] = string(decoded)
					}
				}
			}

			fmt.Printf("#\t%s:\n", key)
			for _, element := range jsonValue {
				for jkey, jvalue := range element {
					fmt.Printf("#\t\t%s: %s\n", jkey, jvalue)
				}
			}
		}

	} else {
		fmt.Printf("# multipart error: %s\n", err)
	}

	body, err := ioutil.ReadAll(request.Body)

	if len(body) > 0 {
		fmt.Printf("# body: %s\n", body)
	}

	fmt.Printf("######\n\n\n")

	if err != nil {
		fmt.Printf("Error reading body: %s\n", err)
	}

	fmt.Fprintf(writer, "{\"success\":\"true\"}")
}

func main() {
	http.HandleFunc("/datastore", display)

	err := http.ListenAndServe(":8000", nil)
	if err != nil {
		fmt.Printf("Error serving: %s\n", err)
	}
}
