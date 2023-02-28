### local
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

### setting env on aws client machine/ aws_server machine
```bash
bash aws_env.sh
```

### run server nodes on aws client machine/ aws_server machine
```bash
bash nodes_run.sh
```

### run client on aws client machine (test/ submit)
```bash
java -jar a7_2023_eval_tests_v1.jar -servers-list=servers.txt
java -jar a7_2023_eval_tests_v1.jar -servers-list=servers.txt -submit -secret-code 5709282193
```