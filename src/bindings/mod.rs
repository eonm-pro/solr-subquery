use crate::errors::SolrSubqueryError;
use crate::query_chain::QueryChain as Chain;
use crate::solr_query::SubQuery;
use crate::SolrQuery as Query;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

impl From<SolrSubqueryError> for PyErr {
    fn from(err: SolrSubqueryError) -> PyErr {
        PyValueError::new_err(err.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct SolrQuery {
    pub query: Query,
}

#[pymethods]
impl SolrQuery {
    #[new]
    fn new(url: &str) -> Result<SolrQuery, SolrSubqueryError> {
        Ok(SolrQuery {
            query: Query::new(url)?,
        })
    }

    fn inner_join(&self, query: SolrQuery) -> Result<SolrQuery, SolrSubqueryError> {
        Ok(SolrQuery {
            query: self.query.inner_join(&query.into())?,
        })
    }

    fn inverse(&self) -> SolrQuery {
        SolrQuery {
            query: self.query.inverse(),
        }
    }

    fn url(&self) -> String {
        self.query.url.to_string()
    }
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct SolrQueryChain {
    chain: Chain,
}

#[pymethods]
impl SolrQueryChain {
    #[new]
    fn new(queries: Vec<SolrQuery>) -> SolrQueryChain {
        let queries = queries.into_iter().map(|q| q.query).collect();

        SolrQueryChain {
            chain: Chain::new(queries),
        }
    }

    fn add_subquery(&mut self, url: &str) -> Result<(), SolrSubqueryError> {
        self.chain.add_subquery(url)?;
        Ok(())
    }

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<SolrQuery> {
        slf.chain.next().map(|q| q.into())
    }
}

impl From<Query> for SolrQuery {
    fn from(query: Query) -> SolrQuery {
        SolrQuery { query }
    }
}

impl From<SolrQuery> for Query {
    fn from(query: SolrQuery) -> Query {
        query.query
    }
}

impl From<Chain> for SolrQueryChain {
    fn from(chain: Chain) -> SolrQueryChain {
        SolrQueryChain { chain }
    }
}

#[pymodule]
fn solr_subquery(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<SolrQuery>()?;
    m.add_class::<SolrQueryChain>()?;
    Ok(())
}
