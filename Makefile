
all: get-deps build

get-deps:
	rebar get-deps

build:
	rebar compile
