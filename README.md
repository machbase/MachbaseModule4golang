# golang Machbase module example

This page is an example of 'How to use a Machbase module fo golang' posted on the Machbase Tech blog.

Machbase Tech blog address is: https://www.machbase.com/kr/howtouse/?mod=document&pageid=1&uid=117

<pre>
main.go              : example source
machbase/machbase.go : Machbase module fo golang
</pre>

<b>Environment Settings</b>

If the machbase is installed in the $MACHBASE_HOME folder, add the environment variable as follows:
<pre>
export LD_LIBRARY_PATH=$MACHBASE_HOME/lib:$LD_LIBRARY_PATH
export CGO_CFLAGS="-I$MACHBASE_HOME/include"
export CGO_LDFLAGS="-L$MACHBASE_HOME/lib -lmachbasecli_dll"
</pre>
