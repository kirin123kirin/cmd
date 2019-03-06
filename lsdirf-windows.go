package main

import (
    "fmt"
    "os"
    "path/filepath"
    "regexp"
    "io/ioutil"
    "reflect"
    "strings"
    "golang.org/x/text/encoding/japanese"
    "golang.org/x/text/transform"
)

var excludeDir = regexp.MustCompile(`^(\.svn|\.git)$`)
var excludeFile = regexp.MustCompile(`(^\~\$|Thumbs.db|\.swp$)`)
var sep = "\t"
var header = true

type Info_t struct {
	mode string
    uname string
    gname string
    mtime string
    size string
    ext string
    name string
    fullpath string
    link string
}

func utf8_to_sjis(str string) (string) {
        iostr := strings.NewReader(str)
        rio := transform.NewReader(iostr, japanese.ShiftJIS.NewEncoder())
        ret, err := ioutil.ReadAll(rio)
        if err != nil {
                return ""
        }
        return string(ret)
}

func is_dir(name string) bool {
    fi, err := os.Stat(name)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        return false
    }
    return fi.IsDir()
}

func getlinkpath(name string) string {
	fi, err := os.Lstat(name)
    if err != nil {
        return ""
    }
    if fi.Mode() & os.ModeSymlink != 0 {
	    a, _ := filepath.Abs(name)
	    rn, err := filepath.EvalSymlinks(a)
	    if err != nil {
		    fmt.Fprintln(os.Stderr, err)
		    return ""
	    }
	    return rn
	}
	return ""
}

func render(name string) {
    fi, err := os.Stat(name)
    info := Info_t{}
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
    } else {
        info.mode = fi.Mode().String()
        info.mtime = fi.ModTime().Format("2006-01-02 15:04")
        info.size = fmt.Sprint(fi.Size())
        info.ext = filepath.Ext(name)
        info.name = fi.Name()
        info.fullpath, _ = filepath.Abs(name)
        info.link = getlinkpath(name)
    }

    if header {
        printHeader()
        header = false
    }

    printInfo(info)
}

func printInfo(info Info_t) {
	v := reflect.Indirect(reflect.ValueOf(info))
    t := v.Type()
	for i := 0; i < t.NumField(); i++ {
	    fmt.Printf("%s" + sep, utf8_to_sjis(v.Field(i).String()))
	}
	fmt.Println("")
}

func printHeader() {
	t := reflect.Indirect(reflect.ValueOf(Info_t{})).Type()
	for i := 0; i < t.NumField(); i++ {
	    fmt.Printf("%s" + sep, t.Field(i).Name)
	}
	fmt.Println("")
}

func handler(searchPath string) error {
    files, err := ioutil.ReadDir(searchPath)
    if err != nil {
        render(searchPath)
        return nil
    }
    
    for _, file := range files {
        b := file.Name()
        abs := filepath.Join(searchPath, b)
        
        if excludeDir.MatchString(b) || excludeFile.MatchString(b) {
            continue
        }
        
        if is_dir(abs) {
            handler(abs)
            continue
        }
        
        render(abs)
    }
    return nil
}

func main() {
	if len(os.Args) > 1 {
	    for _, v := range os.Args[1:] {
		    files, _ := filepath.Glob(v)
		    for _, g := range files {
			    handler(g)
		    }
	    }
	} else {
        handler("./")
    }
}


