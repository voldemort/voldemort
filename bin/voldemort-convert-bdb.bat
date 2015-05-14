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
set JVM_OPTS=-server -Xms2g -Xmx2g -XX:NewSize=1024m -XX:MaxNewSize=1024m -XX:+AlwaysPreTouch -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -XX:SurvivorRatio=2

java -Dlog4j.debug -Dlog4j.configuration="file:/%BASE_DIR%\src\java\log4j.properties" %JVM_OPTS% -cp %CLASSPATH% voldemort.store.bdb.dataconversion.BdbConvertData %*

endlocal
:eof