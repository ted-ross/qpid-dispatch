# dispatch-standalone
The stand-alone qpid dispatch console is an html web site that monitors and controls a qpid dispatch router

## To install the console:

  After a build, the console files are normally installed under /usr/share/qpid-dispatch/console/stand-alone
  The 3rd party libraries are installed at during a 'make install' if npm is present on the build machine.

  ### To manually install the 3rd party libraries:
    - cd /usr/share/qpid-dispatch/console/stand-alone
    - npm install

Note: An internet connection is required during the npm install to retrieve the 3rd party javascript / css files.

## To run the web console:
- Ensure one of the routers in your network is configured with a normal listener with http: true
listener {
    role: normal
    host: 0.0.0.0
    port: 5673
    http: true
    saslMechanisms: ANONYMOUS
}
- start the router
- in a browser, navigate to http://localhost:5673/

The router will serve the console's html/js/css from the install directory.
The cosole will automatically connect to the router at localhost:5673


