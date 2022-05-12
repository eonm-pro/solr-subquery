use std::collections::VecDeque;

use crate::errors::SolrSubqueryError;
use crate::solr_query::{SolrQuery, SubQuery};
use url::Url;

#[derive(Debug, Clone, PartialEq)]
/// Chain multiple Solr queries together
pub struct QueryChain {
    queries: VecDeque<SolrQuery>,
    iteration: usize,
}

impl QueryChain {
    pub fn new(queries: Vec<SolrQuery>) -> QueryChain {
        QueryChain {
            queries: queries.into(),
            iteration: 0,
        }
    }

    pub fn add_subquery<U: TryInto<Url>>(&mut self, url: U) -> Result<(), SolrSubqueryError>
    where
        <U as TryInto<Url>>::Error: std::error::Error + 'static,
    {
        let query = SolrQuery::new(url)?;
        self.queries.push_back(query);
        Ok(())
    }
}

impl Iterator for QueryChain {
    type Item = SolrQuery;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iteration == 0 {
            self.iteration += 1;
            return self.queries.get(0).cloned();
        }

        match (self.queries.pop_front(), self.queries.pop_front()) {
            (Some(q1), Some(q2)) => {
                self.iteration += 1;
                let new_query = q1.inner_join(&q2).unwrap();
                self.queries.push_front(new_query.clone());
                Some(new_query)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod query_chain_tests {
    use super::*;
    use std::error::Error;
    use urlencoding::decode;

    #[test]

    fn test_chain() -> Result<(), Box<dyn Error>> {
        let first_query = SolrQuery::new("http://localhost:8983/solr/collection/select?q=1:*")?;
        let second_query = SolrQuery::new("http://localhost:8983/solr/collection/select?q=2:*")?;
        let third_query = SolrQuery::new("http://localhost:8983/solr/collection/select?q=3:*")?;

        let mut query_chain = QueryChain::new(vec![first_query, second_query, third_query]);

        let first_query = query_chain.next();
        let first_query_string = first_query.unwrap().url.to_string();
        let first_query_result = decode(&first_query_string)?;

        assert_eq!(
            first_query_result,
            "http://localhost:8983/solr/collection/select?q=1:*"
        );

        let second_query = query_chain.next();
        let second_query_string = second_query.unwrap().url.to_string();
        let second_query_result = decode(&second_query_string)?;

        assert_eq!(
            second_query_result,
            "http://localhost:8983/solr/collection/select?q=(1:*)+AND+(2:*)"
        );

        let third_query = query_chain.next();
        let third_query_string = third_query.unwrap().url.to_string();
        let third_query_result = decode(&third_query_string)?;

        assert_eq!(
            third_query_result,
            "http://localhost:8983/solr/collection/select?q=((1:*)+AND+(2:*))+AND+(3:*)"
        );

        let fourth_query = query_chain.next();
        assert_eq!(fourth_query, None);

        Ok(())
    }
}
