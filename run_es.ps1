Write-Host "Starting elasticsearch script"

Invoke-WebRequest "https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.6.0.zip" -OutFile .\es16.zip;

$destFolder = "$pwd\es16\elasticsearch-1.6.0";

$shell = new-object -com shell.application;


$zip = $shell.NameSpace("$pwd\es16.zip");

if (Test-Path $pwd\$destFolder )
{
	del $pwd\$destFolder -Force -Recurse
}

md ".\es16";

foreach($item in $zip.items())
{
	$shell.Namespace("$pwd\es16").copyhere($item);
}

cd $destFolder	

.\bin\service.bat install

.\bin\service.bat start

cd ..
cd ..