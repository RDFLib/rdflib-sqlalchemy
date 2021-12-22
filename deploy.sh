#!/bin/bash -xe
HEAD_REV=$(git rev-parse HEAD)

git log --format=%s -n 1 "$HEAD_REV" | grep -q '^MINOR:' && exit 0

date=$(date +"%Y%m%d%H%M%S")
sed -i -r 's/^version = "([^"]+)(\.dev|a)0"$/version = "\1\2'$date'"/' setup.py
git diff --quiet setup.py && echo "Unsupported version format (release?)" && exit 0
python setup.py egg_info sdist bdist_wheel
twine upload -c "Built by CI. Uploaded after $(date +"%Y-%m-%d %H:%M:%S")" dist/rdflib-sqlalchemy*tar.gz dist/rdflib_sqlalchemy*whl
