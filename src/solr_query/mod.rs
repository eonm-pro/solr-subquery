use crate::errors::*;

use url::Url;

#[derive(Debug, Clone, PartialEq)]
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
    negation: Url,
}

pub trait QueryParam {
    fn params(&self, param_name: &str) -> Vec<String>;
    fn set_param(&mut self, param: (&str, &str));
}

impl QueryParam for Url {
    fn params(&self, param_name: &str) -> Vec<String> {
        self.query_pairs()
            .filter(|(k, _)| k == param_name)
            .map(|(_, v)| v.to_string())
            .collect::<Vec<String>>()
    }

    fn set_param(&mut self, param: (&str, &str)) {
        let clone_url = self.clone();
        let parameters = clone_url.query_pairs().collect::<Vec<_>>();
        let mut url_query_pairs = self.query_pairs_mut();
        url_query_pairs.clear();

        for (key, value) in &parameters {
            if key == param.0 {
                url_query_pairs.append_pair(key, param.1);
            } else {
                url_query_pairs.append_pair(key, value);
            }
        }

        drop(url_query_pairs);
    }
}

impl SolrQuery {
    /// Creates a new SolrQuery from an URL or a string
    pub fn new<U: TryInto<Url>>(url: U) -> Result<SolrQuery, SolrSubqueryError>
    where
        <U as TryInto<Url>>::Error: std::error::Error + 'static,
    {
        let url: Url = url
            .try_into()
            .map_err(|e| SolrSubqueryError::InvalidUrl(e.to_string()))?;

        let mut negation_url = url.clone();
        let q_params = negation_url.params("q");

        match q_params.len() {
            0 => Err(SolrSubqueryError::MissingQQueryParameter),
            1 => {
                let q = format!("{} ({})", Operator::Not, q_params[0]);
                negation_url.set_param(("q", &q));

                Ok(SolrQuery {
                    url,
                    negation: negation_url,
                })
            }
            _ => Err(SolrSubqueryError::MultipleQQueryParameters),
        }
    }

    /// Gets the left join if null query
    pub fn inverse(&self) -> SolrQuery {
        SolrQuery {
            url: self.negation.clone(),
            negation: self.url.clone(),
        }
    }

    fn q_param(&self) -> Result<String, SolrSubqueryError> {
        let q_params = self.url.params("q");

        match q_params.len() {
            0 => Err(SolrSubqueryError::MissingQQueryParameter),
            1 => Ok(q_params[0].clone()),
            _ => Err(SolrSubqueryError::MultipleQQueryParameters),
        }
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
    fn check_has_same_port(&self, other: &SolrQuery) -> Result<(), SolrSubqueryError>;
}

impl SubQuery for SolrQuery {
    fn merge_queries(
        &self,
        other: &SolrQuery,
        operator: Operator,
    ) -> Result<SolrQuery, SolrSubqueryError> {
        self.check_has_same_host(other)?;
        self.check_has_same_port(other)?;
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
            negation: negative.url,
        })
    }

    fn check_has_same_host(&self, other: &SolrQuery) -> Result<(), SolrSubqueryError> {
        if self.url.host() == other.url.host() {
            Ok(())
        } else {
            Err(SolrSubqueryError::DifferentsHosts(
                self.url.host_str().map(|h| h.to_string()),
                other.url.host_str().map(|h| h.to_string()),
            ))
        }
    }

    fn check_has_same_path(&self, other: &SolrQuery) -> Result<(), SolrSubqueryError> {
        if self.url.path() == other.url.path() {
            Ok(())
        } else {
            Err(SolrSubqueryError::DifferentsPaths)
        }
    }

    fn check_has_same_port(&self, other: &SolrQuery) -> Result<(), SolrSubqueryError> {
        if self.url.port() == other.url.port() {
            Ok(())
        } else {
            Err(SolrSubqueryError::DifferentsPorts(
                self.url.port(),
                other.url.port(),
            ))
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

        let negation_url_string = inner_join_query.inverse().url.to_string();
        let negation_result = decode(&negation_url_string)?;
        let negation_expected = "http://localhost:8983/solr/collection1/select?q=(1:*)+NOT+(2:*)";

        assert_eq!(negation_result, negation_expected);

        Ok(())
    }

    #[test]
    fn should_multi_chain() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=1:*")?;
        let second_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=2:*")?;

        let negation = first_query.inverse();
        let negation_url = negation.url.to_string();
        let negation_result = decode(&negation_url)?;

        let negation_expected = "http://localhost:8983/solr/collection1/select?q=NOT+(1:*)";
        
        assert_eq!(negation_result, negation_expected);

        let inner_join = first_query.inner_join(&second_query)?;
        let url_string = inner_join.url.to_string();
        let result = decode(&url_string)?;

        let expected = "http://localhost:8983/solr/collection1/select?q=(1:*)+AND+(2:*)";
        assert_eq!(result, expected);

        let inverse_url_string = inner_join.inverse().url.to_string();
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

        let inverse_url_string = inner_join_query.inverse().url.to_string();
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

        let inverse_url_string = inner_join_query.inverse().url.to_string();
        let inverse_result = decode(&inverse_url_string)?;
        let inverse_expected = "http://localhost:8983/solr/collection1/select?q=(((1:*)+AND+(2:*))+AND+(3:*))+NOT+(4:*)";

        assert_eq!(inverse_result, inverse_expected);

        Ok(())
    }

    #[test]
    fn should_not_inner_join_queries_with_differents_hosts() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost1:8983/solr/collection1/select?q=*:*")?;
        let second_query = SolrQuery::new("http://localhost2:8983/solr/collection1/select?q=*:*")?;

        let inner_join_query = first_query.inner_join(&second_query);

        assert_eq!(
            inner_join_query,
            Err(SolrSubqueryError::DifferentsHosts(
                Some("localhost1".into()),
                Some("localhost2".into())
            ))
        );
        Ok(())
    }

    #[test]
    fn should_not_inner_join_queries_with_differents_ports() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=*:*")?;
        let second_query = SolrQuery::new("http://localhost:8984/solr/collection1/select?q=*:*")?;

        let inner_join_query = first_query.inner_join(&second_query);

        assert_eq!(
            inner_join_query,
            Err(SolrSubqueryError::DifferentsPorts(Some(8983), Some(8984)))
        );
        Ok(())
    }

    #[test]
    fn should_not_inner_join_queries_with_differents_paths() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost:8983/solr/collection1/select?q=*:*")?;
        let second_query = SolrQuery::new("http://localhost:8983/solr/collection2/select?q=*:*")?;

        let inner_join_query = first_query.inner_join(&second_query);

        assert_eq!(inner_join_query, Err(SolrSubqueryError::DifferentsPaths));
        Ok(())
    }

    #[test]
    fn should_not_inner_join_queries_without_q_param() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost:8983/solr/collection/select");
        assert_eq!(first_query, Err(SolrSubqueryError::MissingQQueryParameter));
        Ok(())
    }

    #[test]
    fn should_not_inner_join_queries_with_multiple_q_params() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost:8983/solr/collection/select?q=1&q=2");
        assert_eq!(
            first_query,
            Err(SolrSubqueryError::MultipleQQueryParameters)
        );
        Ok(())
    }
}
