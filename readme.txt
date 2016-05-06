Steps:
1. Ubuntu 14.04 contains a version of Node.js in its default repositories,Install Node.js by typing: "sudo apt-get install nodejs"
2. Install Node.js package manager by typing: "sudo apt-get install npm"
3. Go to source location "server directory path"
4. Open config.js change IP address , Port ,Access key and Secret key as required.
5. Run command "sudo npm install forever -g"
6. Run command "npm install"
7. Run command "forever start server.js"