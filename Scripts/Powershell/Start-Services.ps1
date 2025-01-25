Get-Service -Name SQLAgent$SQL2016 |Set-Service -Status Running
Get-Service -Name SQLAgent$SQL2019 |Set-Service -Status Running
Get-Service -Name MSOLAP$SQL2016 |Set-Service -Status Running
