use std::error::Error;

#[derive(Debug, Clone, PartialEq)]
/// All possible errors that can occur
pub enum SolrSubqueryError {
    /// The URL is not valid
    InvalidUrl(String),
    /// Request has no `q` parameter
    MissingQQueryParameter,
    /// Request has multiple `q` parameters
    MultipleQQueryParameters,
    //// Requests have different hosts
    DifferentHosts,
    //// Requests have different paths
    DifferentsPaths,
}

impl std::fmt::Display for SolrSubqueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SolrSubqueryError::InvalidUrl(e) => write!(f, "Invalid URL: {}", e),
            SolrSubqueryError::MissingQQueryParameter => {
                write!(f, "Request has no `q` query parameter")
            }
            SolrSubqueryError::MultipleQQueryParameters => {
                write!(f, "Request has multiple `q` query parameters")
            }
            SolrSubqueryError::DifferentHosts => write!(f, "Requests have different hosts"),
            SolrSubqueryError::DifferentsPaths => write!(f, "Requests have different paths"),
        }
    }
}

impl Error for SolrSubqueryError {}
