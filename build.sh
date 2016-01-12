#!/bin/sh
rm -rf output/binlogsync.tar.gz 
find ./ -type d -name .svn |xargs -i rm -rf {}
mkdir output
cd ..
tar zcf binlogsync.tar.gz binlogsync/bin binlogsync/conf binlogsync/data binlogsync/monitor binlogsync/hook
mv binlogsync.tar.gz binlogsync/output/
