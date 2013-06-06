PLT_APPS ?=
SRC ?= src
EBIN ?= ebin deps/*/ebin
DIALYZER_OPTS ?= \
	-Werror_handling\
	-Wunmatched_returns
REBAR ?= ./rebar

.PHONY: all compile deps demo-shell shell test rel relclean pltclean dialyze

all: compile

compile: deps
	$(REBAR) compile

deps:
	$(REBAR) get-deps

demo-shell: compile
	erl -pa $(EBIN) -s $(PROJECT)

shell: compile
	erl -pa $(EBIN)

test:
	$(REBAR) eunit skip_deps=true

rel: compile
	$(REBAR) generate

relclean:
	rm -rf rel/$(PROJECT)

# Dialyzer.

.$(PROJECT).plt:
	@dialyzer --build_plt --output_plt .$(PROJECT).plt \
	    --apps erts kernel stdlib $(PLT_APPS)

dialyze: .$(PROJECT).plt
	@dialyzer --src $(SRC) \
	    --plt .$(PROJECT).plt $(DIALYZER_OPTS)

pltclean:
	rm .$(PROJECT).plt
