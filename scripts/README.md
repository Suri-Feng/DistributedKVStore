### Local Machine 
- edit params in info.txt
- move following file to the same folder
  - client jar 
  - server jar 
  - cpen431_pop.pub 
  - aws pem to the same folder
- transport all files 
  ```bash
  aws_files_transport.sh
  ```
- ssh
  ```bash
  ssh -i <pem> ubuntu@<public_ip>
  ```
- after the test/submit, transfer log file back
  ```bash
  scp -i <pem> ubuntu@<public_ip>:<logfile> <logfile>
  ```
- TODO: automate the process after transport files 

### AWS Remote Machines 
- setting env on aws client machine/ aws server machine
  ```bash
  bash aws_env_client.sh
  bash aws_env_server.sh
  ```
- run server nodes on aws client machine/ aws server machine
  ```bash
  bash nodes_run.sh
  ```
- run client on aws client machine (test/ submit)
  ```bash
  java -jar <client_jar> -servers-list=servers.txt
  java -jar <client_jar> -servers-list=servers.txt -submit -secret-code 5709282193
  ```
- clean 
  ```bash
  bash nodes_shundown.sh
  sudo tc qdisc del dev <iface> root
  ```

### Command Shortcuts
