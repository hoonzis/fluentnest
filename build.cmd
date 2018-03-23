@echo off

setlocal

call %~dp0\paket.cmd restore --silent

dotnet build