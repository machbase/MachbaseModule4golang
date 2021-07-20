# golang Machbase module example

This page is an example of 'How to use a Machbase module fo golang' posted on the Machbase Tech blog.

Machbase Tech blog address is: https://kr.machbase.com/machbase-go-golang-%eb%aa%a8%eb%93%88/

<pre>
machbase 1.0 : use machbase cli
machbase 1.5 : use machbase cli 
machbase 6.5 : use machbase rest-api
</pre>

<pre>
main.go              : example source
machbase/machbase.go : Machbase module fo golang
</pre>

<b>Environment Settings</b>

If the machbase is installed in the $MACHBASE_HOME folder, add the environment variable as follows:
This environment variable is only for module version 1.0 and 1.5
<pre>
export LD_LIBRARY_PATH=$MACHBASE_HOME/lib:$LD_LIBRARY_PATH
export CGO_CFLAGS="-I$MACHBASE_HOME/include"
export CGO_LDFLAGS="-L$MACHBASE_HOME/lib -lmachbasecli_dll"
</pre>
