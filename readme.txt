Steps:
1. Ubuntu 14.04 contains a version of Node.js in its default repositories,Install Node.js by typing: "sudo apt-get install nodejs"
2. Install Node.js package manager by typing: "sudo apt-get install npm"
3. Go to source location server directory path run command "cd Server"
4. Open config.js change IP address , Port ,S3_Endpoint as required.
5. To set node run command "sudo ln -s /usr/bin/nodejs /usr/bin/node"
6. Run command "sudo npm install forever -g"
7. Run command "sudo npm install"
8. Run command "forever start server.js"