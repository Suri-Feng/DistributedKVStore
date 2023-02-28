### local
- edit params in info.txt
- move following file to the same folder
  - client jar 
  - server jar 
  - cpen431_pop.pub 
  - aws pem to the same folder
```bash
aws_files_transport.sh
```

### client
```bash
bash aws_env.sh
bash nodes_run.sh
```

### server
```bash
bash aws_env.sh
bash nodes_run.sh
```

5709282193

### test/ submit on client
```bash
java -jar a7_2023_eval_tests_v1.jar -servers-list=servers.txt
java -jar a7_2023_eval_tests_v1.jar -servers-list=servers.txt -submit -secret-code 5709282193
```