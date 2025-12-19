default:
    just -l
install:
    gh install .
run: build
    gh actstat
build:
    go build
