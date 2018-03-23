Write-Host "Starting elasticsearch script"

Invoke-WebRequest "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.2.3.zip" -OutFile .\es.zip;

$destFolder = "$pwd\elasticsearch-6.2.3";

$shell = new-object -com shell.application;

if (Test-Path $destFolder )
{
	del $destFolder -Force -Recurse
}

Expand-Archive -Path es.zip -DestinationPath $pwd

$elasticsearch = "$destFolder\bin\elasticsearch.bat"
$arguments = "-d"
Start-Process -NoNewWindow $elasticsearch $arguments
