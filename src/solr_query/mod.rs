use crate::errors::*;

use url::Url;

/// A Solr boolean operator
pub enum Operator {
    And,
    Or,
    Not,
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Operator::And => write!(f, "AND"),
            Operator::Or => write!(f, "OR"),
            Operator::Not => write!(f, "NOT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A Solr query
pub struct SolrQuery {
    pub url: Url,
    negation: Option<Url>,
}

impl SolrQuery {
    /// Creates a new SolrQuery from an URL or a string
    pub fn new<U: TryInto<Url>>(url: U) -> Result<SolrQuery, SolrSubqueryError>
    where
        <U as TryInto<Url>>::Error: std::error::Error + 'static,
    {
        Ok(SolrQuery {
            url: url
                .try_into()
                .map_err(|e| SolrSubqueryError::InvalidUrl(e.to_string()))?,
            negation: None,
        })
    }

    /// Gets the left join if null query
    pub fn inverse(&self) -> Option<SolrQuery> {
        self.negation.as_ref().map(|url| SolrQuery {
            url: url.clone(),
            negation: None,
        })
    }

    fn q_param(&self) -> Result<String, SolrSubqueryError> {
        let mut q_params = self
            .url
            .query_pairs()
            .filter(|(k, _)| k == "q")
            .map(|(_, v)| v)
            .collect::<Vec<_>>();

        if q_params.is_empty() {
            return Err(SolrSubqueryError::MissingQQueryParameter);
        }

        if q_params.len() > 1 {
            return Err(SolrSubqueryError::MultipleQQueryParameters);
        }

        Ok(q_params
            .pop()
            .map(|q| q.to_string())
            .expect("q_param is expected"))
    }
}

pub trait SubQuery {
    fn merge_queries(
        &self,
        query: &SolrQuery,
        operator: Operator,
    ) -> Result<SolrQuery, SolrSubqueryError>;
    fn inner_join(&self, other: &SolrQuery) -> Result<SolrQuery, SolrSubqueryError>;
    fn check_has_same_path(&self, other: &SolrQuery) -> Result<(), SolrSubqueryError>;
    fn check_has_same_host(&self, other: &SolrQuery) -> Result<(), SolrSubqueryError>;
}

impl SubQuery for SolrQuery {
    fn merge_queries(
        &self,
        other: &SolrQuery,
        operator: Operator,
    ) -> Result<SolrQuery, SolrSubqueryError> {
        self.check_has_same_host(other)?;
        self.check_has_same_path(other)?;

        let self_q = self.q_param()?;
        let other_q = other.q_param()?;

        let mut new_url = other.url.clone();
        let mut new_url_query_pairs = new_url.query_pairs_mut();
        new_url_query_pairs.clear();

        let new_q_param = format!("({}) {} ({})", self_q, operator, other_q);

        for (key, value) in other.url.query_pairs() {
            if key != "q" {
                new_url_query_pairs.append_pair(&key, &value);
            } else {
                new_url_query_pairs.append_pair("q", &new_q_param);
            }
        }

        drop(new_url_query_pairs);

        SolrQuery::new(new_url)
    }

    fn inner_join(&self, other: &SolrQuery) -> Result<SolrQuery, SolrSubqueryError> {
        let positive = self.merge_queries(other, Operator::And)?;
        let negative = self.merge_queries(other, Operator::Not)?;

        Ok(SolrQuery {
            url: positive.url,
            negation: Some(negative.url),
        })
    }

    fn check_has_same_host(&self, other: &SolrQuery) -> Result<(), SolrSubqueryError> {
        if self.url.host() == other.url.host() {
            Ok(())
        } else {
            Err(SolrSubqueryError::DifferentHosts)
        }
    }

    fn check_has_same_path(&self, other: &SolrQuery) -> Result<(), SolrSubqueryError> {
        if self.url.path() == other.url.path() {
            Ok(())
        } else {
            Err(SolrSubqueryError::DifferentsPaths)
        }
    }
}

#[cfg(test)]
mod solr_query_tests {
    use super::*;
    use std::error::Error;
    use urlencoding::decode;

    #[test]
    fn should_inner_join_queries() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=1:*")?;
        let second_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=2:*")?;

        let inner_join_query = first_query.inner_join(&second_query)?;

        let url_string = inner_join_query.url.to_string();
        let result = decode(&url_string)?;
        let expected = "http://localhost:8983/solr/collection1/select?q=(1:*)+AND+(2:*)";
        assert_eq!(result, expected);

        let negation_url_string = inner_join_query.inverse().unwrap().url.to_string();
        let negation_result = decode(&negation_url_string)?;
        let negation_expected = "http://localhost:8983/solr/collection1/select?q=(1:*)+NOT+(2:*)";

        assert_eq!(negation_result, negation_expected);

        Ok(())
    }

    #[test]
    fn should_multi_chain() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=1:*")?;
        let second_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=2:*")?;

        assert!(first_query.inverse().is_none());
        assert!(second_query.inverse().is_none());

        let inner_join = first_query.inner_join(&second_query)?;
        let url_string = inner_join.url.to_string();
        let result = decode(&url_string)?;

        let expected = "http://localhost:8983/solr/collection1/select?q=(1:*)+AND+(2:*)";
        assert_eq!(result, expected);

        let inverse_url_string = inner_join.inverse().unwrap().url.to_string();
        let inverse_result = decode(&inverse_url_string)?;
        let inverse_expected = "http://localhost:8983/solr/collection1/select?q=(1:*)+NOT+(2:*)";

        assert_eq!(inverse_result, inverse_expected);

        let third_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=3:*")?;

        let inner_join_query = first_query
            .inner_join(&second_query)?
            .inner_join(&third_query)?;
        let url_string = inner_join_query.url.to_string();
        let result = decode(&url_string)?;
        let expected =
            "http://localhost:8983/solr/collection1/select?q=((1:*)+AND+(2:*))+AND+(3:*)";

        assert_eq!(result, expected);

        let inverse_url_string = inner_join_query.inverse().unwrap().url.to_string();
        let inverse_result = decode(&inverse_url_string)?;
        let inverse_expected =
            "http://localhost:8983/solr/collection1/select?q=((1:*)+AND+(2:*))+NOT+(3:*)";

        assert_eq!(inverse_result, inverse_expected);

        let fourth_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=4:*")?;

        let inner_join_query = inner_join_query.inner_join(&fourth_query)?;
        let url_string = inner_join_query.url.to_string();
        let result = decode(&url_string)?;
        let expected = "http://localhost:8983/solr/collection1/select?q=(((1:*)+AND+(2:*))+AND+(3:*))+AND+(4:*)";

        assert_eq!(result, expected);

        let inverse_url_string = inner_join_query.inverse().unwrap().url.to_string();
        let inverse_result = decode(&inverse_url_string)?;
        let inverse_expected = "http://localhost:8983/solr/collection1/select?q=(((1:*)+AND+(2:*))+AND+(3:*))+NOT+(4:*)";

        assert_eq!(inverse_result, inverse_expected);

        Ok(())
    }

    #[test]
    fn should_not_inner_join_queries_with_different_host() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost1:8983/solr/collection1/select?q=*:*")?;
        let second_query = SolrQuery::new("http://localhost2:8983/solr/collection1/select?q=*:*")?;

        let inner_join_query = first_query.inner_join(&second_query);

        assert_eq!(inner_join_query, Err(SolrSubqueryError::DifferentHosts));
        Ok(())
    }

    #[test]
    fn should_not_inner_join_queries_with_different_path() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=*:*")?;
        let second_query = SolrQuery::new("http://localhost:8983/solr/collection2/select?q=*:*")?;

        let inner_join_query = first_query.inner_join(&second_query);

        assert_eq!(inner_join_query, Err(SolrSubqueryError::DifferentsPaths));
        Ok(())
    }

    #[test]
    fn should_not_inner_join_queries_without_q_param() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost:8983/solr/collection/select")?;
        let second_query = SolrQuery::new("http://localhost:8983/solr/collection/select")?;

        let inner_join_query = first_query.inner_join(&second_query);
        assert_eq!(
            inner_join_query,
            Err(SolrSubqueryError::MissingQQueryParameter)
        );
        Ok(())
    }

    #[test]
    fn should_not_inner_join_queries_with_multiple_q_params() -> Result<(), Box<dyn Error>> {
        let first_query =
            SolrQuery::new("http://localhost:8983/solr/collection/select?q=*:*&q=*:*")?;
        let second_query =
            SolrQuery::new("http://localhost:8983/solr/collection/select?q=*:*&q=*:*")?;

        let inner_join_query = first_query.inner_join(&second_query);
        assert_eq!(
            inner_join_query,
            Err(SolrSubqueryError::MultipleQQueryParameters)
        );
        Ok(())
    }
}
