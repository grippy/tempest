
publish-clean:
	rm -rf target/package

publish-tempest:
	@$(MAKE) publish-clean
	$(shell cd tempest && cargo publish)
