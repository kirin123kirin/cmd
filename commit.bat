@echo off
cd %~dp0
set EDITOR=sakura
git pull
git add -u *
git commit -a
git push -u origin master
