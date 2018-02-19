@echo off

REM ------------------------------------------------------------------------
REM JetStreamQ is a licensed, commercial product, made available from
REM Allure Technology, Inc. See the DISCLAIMER file distributed with this
REM work for more information on warranty, liability, and ownership.
REM For more information, including licensing rights, contact:
REM info@alluretechnology.com
REM
REM http://www.alluretechnology.com
REM
REM This software distributed under the License is distributed on an 
REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
REM express or implied.
REM ------------------------------------------------------------------------

if "%JAVA_HOME%" == "" goto No_Java_Home
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
set JAVA_CMD=%JAVA_HOME%\bin\java.exe
goto run

:No_Java_Home
set JAVA_CMD=java.exe
echo.
echo Warning: JAVA_HOME environment variable is not set.
echo.

:run
set JETSTREAM_OPTS=-DJetStream.logtostdout=true -Xmx256m 

"%JAVA_CMD%" %JETSTREAM_OPTS% -jar bin/jetstream.jar

