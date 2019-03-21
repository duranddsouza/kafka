package com.kafka;

import com.kafka.service.FilterService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FilterServiceTest {

    @Test
    public void test_Filter_Algo() {
        FilterService filterService = new FilterService(52.0d, 3.8d, 51.7d, 4.75d);
        // Inside area
        Assert.assertTrue(filterService.pointInRectangle(51.8, 4.0));
        // Outside area
        Assert.assertFalse(filterService.pointInRectangle(52.5,3.9));
        // On the area boundary fails as per assumed business rules
        Assert.assertFalse(filterService.pointInRectangle(52.0,3.9));
    }

}
