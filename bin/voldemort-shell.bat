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
if %Count% geq 2 goto :continue
echo "USAGE: bin/voldemort-shell.bat store_name bootstrap_url [command_file] [--client-zone-id <zone-id>]"
goto :eof
:continue

setlocal
SET BASE_DIR=%~dp0..

%BASE_DIR%\bin\run-class.bat jline.ConsoleRunner voldemort.VoldemortClientShell %*

endlocal
:eof