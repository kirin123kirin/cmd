@echo off
cd %~dp0

git pull
git add -u .
git commit
git push -u origin master
