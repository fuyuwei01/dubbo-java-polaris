/*
 * Tencent is pleased to support the open source community by making dubbo-polaris-java available.
 *
 * Copyright (C) 2021 Tencent. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.polaris.dubbo.discovery.example.provider;

import com.tencent.polaris.dubbo.example.api.press.PressMetadataPadding;
import org.apache.dubbo.config.ServiceConfig;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Injects generated metadata padding into press {@link ServiceConfig}s before export.
 */
@Component
public class PressMetadataInjector implements BeanPostProcessor {

    private static final String PRESS_INTERFACE_PREFIX =
            "com.tencent.polaris.dubbo.example.api.press.PressService";

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        if (!(bean instanceof ServiceConfig)) {
            return bean;
        }
        ServiceConfig<?> service = (ServiceConfig<?>) bean;
        String iface = service.getInterface();
        if (iface == null || !iface.startsWith(PRESS_INTERFACE_PREFIX)) {
            return bean;
        }
        Map<String, String> parameters = new LinkedHashMap<>();
        if (service.getParameters() != null) {
            parameters.putAll(service.getParameters());
        }
        parameters.putAll(PressMetadataPadding.parameters());
        service.setParameters(parameters);
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }
}
