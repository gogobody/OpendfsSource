how to run:
1. you should compile phxpaxos(github), then replace the lib/* and include/* to what you have compiled.
2. modify /etc/config.h and /etc/*.conf , then run prepareDir.sh
3. cmake . and make 
4. run namenode -h , you should see how to format dfs system
5. run datanode
6. run client -h to see how to use cmds
