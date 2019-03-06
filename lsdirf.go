package main

import (
    "fmt"
    "os"
    "os/user" //unix
    "path/filepath"
    "strings" //unix
    "regexp"
    "io/ioutil"
    "reflect"
    "syscall" //unix
)

var excludeDir = regexp.MustCompile(`^(\.svn|\.git)$`)
var excludeFile = regexp.MustCompile(`(^\~\$|Thumbs.db|\.swp$)`)
var sep = "\t"
var header = true
var usr,_ = user.Current() //unix
var rep = strings.NewReplacer("~/", usr.HomeDir + "/") //unix

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

 //unix
func getuser (stat *syscall.Stat_t) string {
    u, err := user.LookupId(fmt.Sprint(stat.Uid))
    if err == nil {
        return u.Username
    } else {
        return fmt.Sprint(stat.Uid)
    }
}

 //unix
func getgroup(stat *syscall.Stat_t) string {
    g, err := user.LookupGroupId(fmt.Sprint(stat.Gid))
    if err == nil {
        return g.Name
    } else {
        return fmt.Sprint(stat.Gid)
    }
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
        info.fullpath = rep.Replace(info.fullpath) //unix
        stat, ok := fi.Sys().(*syscall.Stat_t) //unix
        if ok { //unix
            info.uname = getuser(stat) //unix
            info.gname = getgroup(stat) //unix
        } //unix
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
	    fmt.Printf("%s" + sep, v.Field(i).String())
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


