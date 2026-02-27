go build -o tunnel.exe
if ($?) {
    .\tunnel.exe $args
}