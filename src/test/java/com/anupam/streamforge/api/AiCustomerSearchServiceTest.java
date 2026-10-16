package com.anupam.streamforge.api;

import java.util.List;
import java.util.Map;

import com.anupam.streamforge.ai.service.AiCustomerSearchService;
import com.anupam.streamforge.config.AiGatewayProperties;
import com.anupam.streamforge.exception.AiGatewayException;
import com.anupam.streamforge.service.CustomerSearchService;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.springframework.http.HttpMethod.POST;

class AiCustomerSearchServiceTest {

    private static final String BASE_URL = "http://localhost:4000";

    @Test
    void extractsFiltersAndSearchesUsingValidatedIntent() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        CustomerSearchService searchService = mock(CustomerSearchService.class);
        CustomerSearchService.SearchResult expectedResults =
                new CustomerSearchService.SearchResult(1, 0, 10, List.of(Map.of("customerId", 1)));
        when(searchService.search("Jane", null, "Austin", "TX", 0, 10)).thenReturn(expectedResults);
        server.expect(requestTo(BASE_URL + "/v1/chat/completions"))
                .andExpect(method(POST))
                .andExpect(header(HttpHeaders.AUTHORIZATION, "Bearer test-key"))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andRespond(withSuccess("""
                        {"choices":[{"message":{"content":"{\\"q\\":\\"Jane\\",\\"city\\":\\"Austin\\",\\"state\\":\\"TX\\"}"}}]}
                        """, MediaType.APPLICATION_JSON));

        AiCustomerSearchService service = new AiCustomerSearchService(
                new AiGatewayProperties(BASE_URL, "test-key", "chat-default"),
                JsonMapper.builder().build(),
                searchService,
                builder);

        AiCustomerSearchService.AiSearchResponse response =
                service.search("Find Jane in Austin, Texas", 0, 10);

        assertEquals(new AiCustomerSearchService.SearchIntent("Jane", null, "Austin", "TX"), response.filters());
        assertEquals(expectedResults, response.results());
        assertEquals(true, response.found());
        verify(searchService).search("Jane", null, "Austin", "TX", 0, 10);
        server.verify();
    }

    @Test
    void searchesByLastNameAndReturnsMessageWhenNothingFound() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        CustomerSearchService searchService = mock(CustomerSearchService.class);
        CustomerSearchService.SearchResult empty = new CustomerSearchService.SearchResult(0, 0, 10, List.of());
        when(searchService.search(null, "Smith", null, null, 0, 10)).thenReturn(empty);
        server.expect(requestTo(BASE_URL + "/v1/chat/completions"))
                .andRespond(withSuccess("""
                        {"choices":[{"message":{"content":"{\\"q\\":null,\\"lastName\\":\\"Smith\\",\\"city\\":null,\\"state\\":null}"}}]}
                        """, MediaType.APPLICATION_JSON));

        AiCustomerSearchService service = new AiCustomerSearchService(
                new AiGatewayProperties(BASE_URL, "test-key", "chat-default"),
                JsonMapper.builder().build(),
                searchService,
                builder);

        AiCustomerSearchService.AiSearchResponse response = service.search("Find customers with last name Smith", 0, 10);

        assertEquals(false, response.found());
        assertEquals("No customers found matching your search.", response.message());
        assertEquals("Smith", response.filters().lastName());
        assertEquals(empty, response.results());
        verify(searchService).search(null, "Smith", null, null, 0, 10);
        server.verify();
    }

    @Test
    void rejectsInvalidModelOutputWithoutSearching() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo(BASE_URL + "/v1/chat/completions"))
                .andRespond(withSuccess("""
                        {"choices":[{"message":{"content":"not-json"}}]}
                        """, MediaType.APPLICATION_JSON));

        AiCustomerSearchService service = new AiCustomerSearchService(
                new AiGatewayProperties(BASE_URL, "test-key", "chat-default"),
                JsonMapper.builder().build(),
                mock(CustomerSearchService.class),
                builder);

        assertThrows(AiGatewayException.class, () -> service.search("Find Jane", 0, 10));
        server.verify();
    }
}
