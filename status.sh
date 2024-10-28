multiLine="";
format="table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}";
docker stats --all --format "$format" --no-trunc | (while read line; do
    sedLine=$(echo "$line" | sed "s/^.*name.*cpu.*mem.*$/_divider_/i")
    if [ "$sedLine" != "_divider_" ];
    then
        multiLine="${multiLine}"'\n'"${line}";
    else
        echo -e $multiLine > $1;
        multiLine="";
    fi;
done);

