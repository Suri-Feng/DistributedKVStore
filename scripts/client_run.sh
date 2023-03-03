META='info.txt'
if [ -f "$META" ]; then
  export jarfile=$(echo `grep -i "Client_jar_file" $META` | cut -d ":" -f 2)
  echo "setting client jarfile variable from info.txt"
else
  echo "$META does not exist. Abort!"
  exit
fi

if [[ $1 == submit ]]; then
    echo "Running in submit mode."
    submit_code=$(echo `grep -i "Submit-secret-code" $META` | cut -d ":" -f 2)
    java -jar $jarfile -servers-list=servers.txt -submit -secret-code $submit_code
else
    echo "Running in test mode."
    java -jar $jarfile -servers-list=servers.txt
fi