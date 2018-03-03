@echo off
cd %~dp0

git pull
git add .
git commit -a
git push -u origin master
pause