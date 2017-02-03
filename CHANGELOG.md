# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [0.4.0] - 2017-02-02
### Added
- #10: FENCEGET command for reading fencing token without changing it (@glycerine)
- Length option to FENCE command, supports batch updates (@glycerine)

### Fixed
- Redcon memory leak

## [0.3.2] - 2016-10-17
### Added
- JSET, JGET, JDEL commands for working with JSON documents.

### Changed
- Moved build.sh into resources direction to avoid confusion about 
which to use: build.sh or make.
- Updated GJSON/SJSON vendored libraries.

## [0.2.2] - 2016-10-17
### Added
- Added BACKUP command
- Added RAFTPEERS command
- Added FENCE command

