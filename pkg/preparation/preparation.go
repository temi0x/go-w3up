package preparation

// HACKME: This package is a placeholder for the preparation package.

// Intended stack:
// initialize a DB
// initialize a sqlrepo.Repo
// initialize configurations.Configurations
// initialize sources.Sources
// initialize uploads.Uploads - ConfigurationSourcesLookup = Configurations.GetSourceIDsForConfiguration
// initialize scans.Scans - SourceAccessor = Sources.GetSourceByID, UploadSourceLookup = Uploads.GetSourceIDForUploadID
// initialize dagscans.DAGScans - FileAccesor = Scans.OpenFileByID, UnixFSParams = Configurations.GetConfigurationByID (TBD)

// put execution code below...
