//! Scan context for pushdown hints to connectors.

use arneb_planner::PlanExpr;

/// Pushdown hints passed to [`DataSource::scan()`](crate::DataSource::scan).
///
/// All fields are optional. A connector that doesn't support a particular
/// pushdown simply ignores that field and performs a full scan.
#[derive(Debug, Clone, Default)]
pub struct ScanContext {
    /// Filter predicates to push down into the scan.
    /// The connector should apply as many as it can; any remaining filters
    /// will still be evaluated by a `FilterExec` above the scan.
    pub filters: Vec<PlanExpr>,

    /// Column indices to project — only these columns need to be returned.
    /// `None` means all columns.
    pub projection: Option<Vec<usize>>,

    /// Maximum number of rows to return.
    pub limit: Option<usize>,
}

impl ScanContext {
    /// Creates an empty scan context (no pushdown).
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the projection columns.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Sets the filter predicates.
    pub fn with_filters(mut self, filters: Vec<PlanExpr>) -> Self {
        self.filters = filters;
        self
    }

    /// Sets the row limit.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Declares which pushdown operations a connector supports.
#[derive(Debug, Clone, Default)]
pub struct ConnectorCapabilities {
    /// Whether the connector can filter rows based on predicates.
    pub supports_filter_pushdown: bool,
    /// Whether the connector can project (select) specific columns.
    pub supports_projection_pushdown: bool,
    /// Whether the connector can limit the number of rows returned.
    pub supports_limit_pushdown: bool,
}
