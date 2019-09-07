#! /bin/bash

# https://stackoverflow.com/questions/19242275/re-error-illegal-byte-sequence-on-mac-os-x
export LC_CTYPE=C;
export LANG=C;

# root workspace
WORKSPACE=`pwd`;

cd $WORKSPACE/tempest-source;
TEMPEST_VERSION=`cargo config package.version | tr -d \"`;

cd $WORKSPACE/tempest-source;
TEMPEST_SOURCE_VERSION=`cargo config package.version | tr -d \"`;

# clean and build book
cd $WORKSPACE/tempest-book;
mdbook clean && mdbook build;

# replace version placeholders
echo "Find and replace book files: TEMPEST_VERSION=$TEMPEST_VERSION";
echo "Find and replace book files: TEMPEST_SOURCE_VERSION=$TEMPEST_SOURCE_VERSION";

cd $WORKSPACE/tempest-book/book;
find . -type f -exec sed -i '' -e "s/TEMPEST_VERSION/$TEMPEST_VERSION/g" {} +;
find . -type f -exec sed -i '' -e "s/TEMPEST_SOURCE_VERSION/$TEMPEST_SOURCE_VERSION/g" {} +;
