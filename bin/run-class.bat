@echo off

REM
REM   Copyright 2008-2009 LinkedIn, Inc
REM
REM  Licensed under the Apache License, Version 2.0 (the "License");
REM   you may not use this file except in compliance with the License.
REM   You may obtain a copy of the License at
REM
REM      http://www.apache.org/licenses/LICENSE-2.0
REM
REM  Unless required by applicable law or agreed to in writing, software
REM  distributed under the License is distributed on an "AS IS" BASIS,
REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM  See the License for the specific language governing permissions and
REM  limitations under the License.
REM

set Count=0
for %%a in (%*) do set /a Count+=1
if %Count% gtr 1 goto :continue
echo %0 java-class-name [options]
goto :eof
:continue

SET BASE_DIR=%~dp0..
SET CLASSPATH=.

set VOLDEMORT_CONFIG_DIR=%1%/config
call %VOLDEMORT_CONFIG_DIR%/voldemort-env.bat

for %%j in (%BASE_DIR%\dist\*.jar) do (call :append_classpath "%%j")
for %%j in (%BASE_DIR%\lib\*.jar) do (call :append_classpath "%%j")
set CLASSPATH=$CLASSPATH:%BASE_DIR%\dist\resources
goto :run

:append_classpath
set CLASSPATH=%CLASSPATH%;%1
goto :eof

:run
if "%VOLD_OPTS%" == "" set "VOLD_OPTS=-Xmx2G -server -Dcom.sun.management.jmxremote"
java -Dlog4j.configuration=%VOLDEMORT_CONFIG_DIR%\log4j.properties %VOLD_OPTS% -cp %CLASSPATH% %*

endlocal
:eof