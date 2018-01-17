@echo off
echo start to package
REM set MAVEN_OPTS=-Xms256m -Xmx512m
REM call mvn clean package -Dmaven.test.skip=true
set currentdir=%~dp0
set targetdir=%currentdir%\target\lppz-spark-saphana-ws
echo %currentdir%

call mvn clean dependency:copy-dependencies -DoutputDirectory=%targetdir%\lib -DincludeScope=compile

call mvn install -Dmaven.test.skip=true

copy %currentdir%\target\*.jar %targetdir%\lib

xcopy %currentdir%\target\props\* %targetdir% /s

copy microrun %targetdir%
copy microstop %targetdir%

echo package successfully
pause
