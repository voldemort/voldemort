@echo off

REM
REM Copyright 2013 Carlos Tasada
REM
REM Licensed under the Apache License, Version 2.0 (the "License");
REM you may not use this file except in compliance with the License.
REM You may obtain a copy of the License at
REM
REM http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.
REM
REM ** This Windows BAT file is not tested with each Voldemort release. **

:: Start up a Voldemort REST Coordinator speaking to a Voldemort Cluster. 
:: Example : 
:: 1. Bring up a Voldemort single node cluster 
::     $ bin/voldemort-server.sh config/single_node_cluster
:: 2. Bring up a Voldemort Coordinator
::     $ bin/voldemort-coordinator.sh config/coordinator_sample_config/config.properties
:: 3. Do operations
::     See config/coordinator_sample_config/curl_samples.txt

set argC=0
for %%a in (%*) do set /a argC+=1
if %argC% geq 1 goto :continue
echo "USAGE: bin/voldemort-coordinator.bat <coordinator-config-file>"
goto :eof
:continue

SET BASE_DIR=%~dp0..
SET CLASSPATH=.

for %%j in ("%BASE_DIR%\dist\*.jar") do (call :append_classpath "%%j")
for %%j in ("%BASE_DIR%\contrib\*\lib\*.jar") do (call :append_classpath "%%j")
for %%j in ("%BASE_DIR%\lib\*.jar") do (call :append_classpath "%%j")
set CLASSPATH=%CLASSPATH%;"%BASE_DIR%\dist\resources"
goto :run

:append_classpath
set CLASSPATH=%CLASSPATH%;%1
goto :eof

:run
if "%VOLD_OPTS%" == "" set "VOLD_OPTS=-Xmx1G -server -Dcom.sun.management.jmxremote"

java -Dlog4j.configuration=src/java/log4j.properties %VOLD_OPTS% -cp %CLASSPATH% voldemort.rest.coordinator.CoordinatorService %*

endlocal
:eof