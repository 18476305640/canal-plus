package com.alibaba.canal.canal.listen;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.canal.canal.listen.annotation.CanalListen;
import com.alibaba.canal.canal.listen.util.CanalAnnotationUtils;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.net.InetSocketAddress;
import java.util.*;

@Component
public class CanalApplicationRunner implements ApplicationRunner {
    private static Map<String,CanalListener> listenerMap = new HashMap<>();
    /**
     * Canal配置
     */
    @Value("${canal.interval-time:1000}")
    public Integer INTERVAL_TIME;
    @Value("${canal.ip:127.0.0.1}")
    public String CANAL_IP_ADDR;
    @Value("${canal.port:11111}")
    public Integer PORT;
    @Value("${canal.password:}")
    public String PASSWORD;
    @Value("${canal.username:}")
    public String USERNAME;
    @Value("${canal.destination:example}")
    public String DESTINATION ;
    @Autowired
    private ApplicationContext applicationContext;

    private String basePackage;

    /**
     * 异步执行运行Canal主方法
     *
     * @param args arg游戏
     * @throws Exception 异常
     */
    @Override
    public void run(ApplicationArguments args) throws Exception {
        // 初始化包
        basePackage = getBasePackage();

        new Thread(()->{
            try {
                canalMain();
            } catch (ClassNotFoundException e) {
                System.err.println("[Canal-Plus] Start Fail !");
                throw new RuntimeException(e);
            }
        }).start();


    }

    /**
     * 获得springboot主启动类所在包
     *
     * @return {@link String}
     */
    private String getBasePackage() {
        // 获取主启动类所在包名
        String mainClassName = null;
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement stackTraceElement : stackTrace) {
            if ("main".equals(stackTraceElement.getMethodName())) {
                mainClassName = stackTraceElement.getClassName();
                break;
            }
        }
        Class<?> mainClass = null;
        try {
            mainClass = Class.forName(mainClassName);
        } catch (ClassNotFoundException e) {
            System.err.println("[Canal ERROR] 获取主启动类包名失败！");
            throw new RuntimeException(e);
        }
        String basePackage = mainClass.getPackage().getName();
        return basePackage;
    }

    /**
     * 获取所有加了@CanalListen("canal_test.t_user") 注解的全类名并映射为 Map （全类名，bean实例）
     * 注意加入@CanalListen("canal_test.t_user") 注解的也相当加了@Component,所以可以从容器中获取对应实例
     *
     * @throws ClassNotFoundException 类没有发现异常
     */
    public void canalMain() throws ClassNotFoundException {
        // 获取所有加了@CanalListen("canal_test.t_user") 注解的方法
        Set<String> listeners = scanCanalListeners(CanalListen.class);
        System.out.println("Scan done target number is  " + listeners.size());
        // 将原本的全类名 -> <pattern,实例>
        listeners.stream().forEach(listenerFullClassName->{
            String pattern = CanalAnnotationUtils.getAnnotationAttrValue(listenerFullClassName, CanalListen.class, String.class, "value");
            try {
                Class<?> clazz = Class.forName(listenerFullClassName);
                CanalListener canalListener = (CanalListener) applicationContext.getBean(clazz);
//            CanalListener canalListener = ReflectionUtils.getInstance(listenerFullClassName, CanalListener.class);
                listenerMap.put(pattern,canalListener);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

        });
        // 得到的 Map （全类名，bean实例） 给一个方法去订阅
        sub(listenerMap);
    }


    /**
     * 订阅，这是Canal的核心逻辑，有消息后映射后之前加了@CanalListen注解的类方法上(handle)
     *
     * @param listenerMap 侦听器地图
     */
    private void sub(Map<String, CanalListener> listenerMap) {
        // db1.table,db2.table
        String subStr = String.join(",", listenerMap.keySet());
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(CANAL_IP_ADDR , PORT),
                DESTINATION,
                USERNAME,
                PASSWORD);
        int batchSize = 1000;
        System.out.println("------ Canal init Success !，开始监听MySQL变化 ------");
        try {
            connector.connect();
            //connector.subscribe(".*\\..*");
            connector.subscribe(subStr);
            connector.rollback();
            while (true) {
                System.out.println("我是Canal，正在监听:"+ UUID.randomUUID());
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try { Thread.sleep(INTERVAL_TIME); } catch (InterruptedException e) { e.printStackTrace(); }
                } else {
                    // 动作处理
                    MotionPerception.handle(message.getEntries(),listenerMap);
                }
                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
        } finally {
            connector.disconnect();
        }



    }


    /**
     * 扫描添加了 @CanalListen 注解的类，并收集这些类的全类名
     *
     * @return 添加了 @CanalListen 注解的类的全类名集合
     * @throws ClassNotFoundException 如果找不到类，则抛出 ClassNotFoundException 异常
     */
    public Set<String> scanCanalListeners(Class<? extends Annotation> annotationType ) throws ClassNotFoundException {
        Set<String> listenerClasses = new HashSet<>();
        // 创建一个递归扫描器，用于递归扫描启动类所在的包路径下的所有类
        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningRecursiveCandidateComponentProvider(false);

        // 设置只扫描添加了 @CanalListen 注解的类
        scanner.addIncludeFilter(new AnnotationTypeFilter(annotationType));

        Set<BeanDefinition> beanDefinitions = scanner.findCandidateComponents(basePackage);

        // 将扫描到的类的全类名添加到 listenerClasses 集合中
        for (BeanDefinition beanDefinition : beanDefinitions) {
            String className = beanDefinition.getBeanClassName();
            // 如果类上添加了 @CanalListen 注解，则将该类的全类名添加到 listenerClasses 集合中
            if (AnnotationUtils.isAnnotationDeclaredLocally(annotationType, Class.forName(className))) {
                listenerClasses.add(className);
            }
        }

        return listenerClasses;
    }
    /**
     * 递归扫描器，用于递归扫描启动类所在的包路径下的所有类
     */
    private static class ClassPathScanningRecursiveCandidateComponentProvider extends ClassPathScanningCandidateComponentProvider {
        public ClassPathScanningRecursiveCandidateComponentProvider(boolean useDefaultFilters) {
            super(useDefaultFilters);
        }
        /**
         * 判断 beanDefinition 是否为候选的组件
         *
         * @param beanDefinition 候选的 bean 定义
         * @return 如果是候选的组件，则返回 true；否则，返回 false。
         */
        @Override
        protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
            // 排除接口和抽象类
            return super.isCandidateComponent(beanDefinition)
                    && (!beanDefinition.getMetadata().isInterface())
                    && (!beanDefinition.getMetadata().isAbstract());
        }
    }
}