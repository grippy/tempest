
publish-clean:
	rm -rf target/package

publish-tempest:
	@$(MAKE) publish-clean
	$(shell cd tempest && cargo publish)

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

build-book:
	./scripts/build-book.sh
