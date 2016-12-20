Write-Host "Starting elasticsearch script"

Invoke-WebRequest "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.1.1.zip" -OutFile .\es.zip;

$destFolder = "$pwd\elasticsearch-5.1.1";

$shell = new-object -com shell.application;

if (Test-Path $destFolder )
{
	del $destFolder -Force -Recurse
}

Expand-Archive -Path es.zip -DestinationPath $pwd

cmd.exe /c "$destFolder\bin\elasticsearch.bat"
