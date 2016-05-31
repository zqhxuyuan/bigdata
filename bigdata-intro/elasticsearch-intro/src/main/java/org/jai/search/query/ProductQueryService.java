package org.jai.search.query;

import org.jai.search.model.*;

import java.util.List;

public interface ProductQueryService
{
    ProductSearchResult searchProducts(SearchCriteria searchCriteria);

    Product getProduct(ElasticSearchIndexConfig config, Long productId);
    
    List<AutoSuggestionEntry> getAutoSuggestions(ElasticSearchIndexConfig config, String queryString);

    List<AutoSuggestionEntry> getAutoSuggestionsUsingTermsFacet(ElasticSearchIndexConfig config, String string);

    List<Product> findSimilarProducts(ElasticSearchIndexConfig config, String[] fields, Long productId);
}
