package io.github.biglv666.cachekit.generic;

import com.fasterxml.jackson.databind.JavaType;
import io.github.biglv666.cachekit.annotation.CacheLoad;
import io.github.biglv666.cachekit.core.BypassContext;
import io.github.biglv666.cachekit.exception.CacheKitException;
import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.Pointcut;
import org.springframework.aop.PointcutAdvisor;
import org.springframework.aop.support.AopUtils;
import org.springframework.aop.support.StaticMethodMatcherPointcut;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code @CacheLoad} 拦截器：按 SpEL 计算 key，命中直接返回，未命中执行用户方法体并回填。
 *
 * <p>key 必须显式配置且表达式结果非空；结果为空视为配置错误直接抛异常，
 * 绝不猜测 key（猜测会把不同参数写到同一个键，比不缓存更危险）。
 * {@code CacheKit.withDb(...)} 作用域内旁路缓存，直接执行方法体。</p>
 *
 * <p>注解查找按<b>目标类</b>解析：方法本身 → 目标类上的具体重写方法 → 实现接口 → 父类。
 * 因此 {@code @CacheLoad} 可以标在接口/泛型父类方法上，实现类无需重复标注；
 * 切点使用自定义 {@link PointcutAdvisor} 而非 {@code @annotation}，避免 CGLIB 代理下
 * 只能匹配具体实现方法注解的问题。类型解析与元数据注册均使用注解声明方法 + 目标类，
 * 保证 {@code T}、{@code List<T>} 按实现类实际实参反序列化。</p>
 */
public class GenericCacheAspect implements PointcutAdvisor, MethodInterceptor {

    private final TieredValueCache tieredCache;
    private final GenericCacheRegistry registry;

    private final SpelExpressionParser spelParser = new SpelExpressionParser();
    private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

    private final Pointcut pointcut = new StaticMethodMatcherPointcut() {
        @Override
        public boolean matches(Method method, Class<?> targetClass) {
            // 桥接方法必须参与匹配：CGLIB 代理下通过接口类型调用时命中的就是桥接方法，
            // 注解会回溯到接口/父类声明；无注解的合成方法由 resolveBinding 返回 null 排除
            return resolveBinding(method, targetClass) != null;
        }
    };

    /** 注解查找结果缓存（代理创建期会逐方法调用 matches） */
    private final Map<MethodKey, Optional<LoadBinding>> bindings = new ConcurrentHashMap<>();
    private final Map<PlanKey, MethodPlan> plans = new ConcurrentHashMap<>();

    public GenericCacheAspect(TieredValueCache tieredCache, GenericCacheRegistry registry) {
        this.tieredCache = tieredCache;
        this.registry = registry;
    }

    @Override
    public Pointcut getPointcut() {
        return pointcut;
    }

    @Override
    public Advice getAdvice() {
        return this;
    }

    @Override
    public boolean isPerInstance() {
        return false;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        if (BypassContext.isActive()) {
            return invocation.proceed();
        }
        Method method = invocation.getMethod();
        Class<?> targetClass = targetClass(invocation, method);
        LoadBinding binding = resolveBinding(method, targetClass);
        if (binding == null) {
            return invocation.proceed();
        }
        Method declaration = binding.declaration();
        MethodPlan plan = plans.computeIfAbsent(new PlanKey(declaration, targetClass),
                ignored -> buildPlan(declaration, targetClass, binding.annotation()));
        Object[] args = invocation.getArguments();
        if (!conditionHolds(plan.condition(), declaration, args)) {
            return invocation.proceed();
        }
        String key = evaluateKey(plan.keyExpression(), declaration, args);
        return tieredCache.loadChecked(plan.metadata(), key, plan.valueType(), plan.ttl(),
                plan.cacheNull(), invocation::proceed);
    }

    private static Class<?> targetClass(MethodInvocation invocation, Method method) {
        Object target = invocation.getThis();
        return target == null ? method.getDeclaringClass() : AopUtils.getTargetClass(target);
    }

    private MethodPlan buildPlan(Method method, Class<?> targetClass, CacheLoad cacheLoad) {
        GenericCacheMetadata metadata = registry.register(method, targetClass, cacheLoad);
        Class<?> explicitValue = cacheLoad.value() == void.class || cacheLoad.value() == Void.class
                ? null
                : cacheLoad.value();
        JavaType valueType = GenericTypeResolver.methodReturnType(method, targetClass, explicitValue);
        Expression keyExpression = spelParser.parseExpression(cacheLoad.key());
        Expression condition = cacheLoad.condition() == null || cacheLoad.condition().isBlank()
                ? null
                : spelParser.parseExpression(cacheLoad.condition());
        Duration ttl = cacheLoad.ttl() > 0 ? Duration.ofSeconds(cacheLoad.ttl()) : null;
        boolean cacheNull = cacheLoad.cacheNull() && metadata.cacheNull();
        return new MethodPlan(metadata, valueType, keyExpression, condition, ttl, cacheNull);
    }

    /**
     * 按目标类查找 {@code @CacheLoad}：方法自身 → 具体重写方法 → 实现接口 → 父类。
     * 返回 null 表示该方法不是通用缓存入口。
     */
    private LoadBinding resolveBinding(Method method, Class<?> targetClass) {
        Class<?> target = targetClass == null ? method.getDeclaringClass() : targetClass;
        return bindings.computeIfAbsent(new MethodKey(method, target),
                ignored -> Optional.ofNullable(findBinding(method, target))).orElse(null);
    }

    private static LoadBinding findBinding(Method method, Class<?> target) {
        LoadBinding direct = bindingOn(method);
        if (direct != null) {
            return direct;
        }
        Method specific = AopUtils.getMostSpecificMethod(method, target);
        LoadBinding onSpecific = specific != method ? bindingOn(specific) : null;
        if (onSpecific != null) {
            return onSpecific;
        }
        String name = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();
        for (Class<?> iface : ClassUtils.getAllInterfacesForClassAsSet(target)) {
            Method declared = ReflectionUtils.findMethod(iface, name, parameterTypes);
            LoadBinding onInterface = declared == null ? null : bindingOn(declared);
            if (onInterface != null) {
                return onInterface;
            }
        }
        for (Class<?> superClass = specific.getDeclaringClass().getSuperclass();
             superClass != null && superClass != Object.class;
             superClass = superClass.getSuperclass()) {
            Method declared = ReflectionUtils.findMethod(superClass, name, parameterTypes);
            LoadBinding onSuper = declared == null ? null : bindingOn(declared);
            if (onSuper != null) {
                return onSuper;
            }
        }
        return null;
    }

    private static LoadBinding bindingOn(Method method) {
        CacheLoad annotation = AnnotatedElementUtils.findMergedAnnotation(method, CacheLoad.class);
        return annotation == null ? null : new LoadBinding(annotation, method);
    }

    private String evaluateKey(Expression expression, Method method, Object[] args) {
        MethodBasedEvaluationContext context =
                new MethodBasedEvaluationContext(null, method, args, parameterNameDiscoverer);
        Object value = expression.getValue(context);
        if (value == null) {
            throw new CacheKitException("@CacheLoad.key 表达式结果为 null，拒绝使用空 key: " + method
                    + " -> " + expression.getExpressionString());
        }
        String key = value instanceof String s ? s : String.valueOf(value);
        if (key.isBlank()) {
            throw new CacheKitException("@CacheLoad.key 表达式结果为空字符串，拒绝使用空 key: " + method
                    + " -> " + expression.getExpressionString());
        }
        return key;
    }

    private boolean conditionHolds(Expression condition, Method method, Object[] args) {
        if (condition == null) {
            return true;
        }
        MethodBasedEvaluationContext context =
                new MethodBasedEvaluationContext(null, method, args, parameterNameDiscoverer);
        Boolean holds = condition.getValue(context, Boolean.class);
        return holds == null || holds;
    }

    /** 注解查找缓存键：同一方法在不同实现类下可能命中不同声明（接口/父类） */
    private record MethodKey(Method method, Class<?> targetClass) {
    }

    /** 计划缓存键：同一接口/父类方法在不同实现类下可能解析出不同泛型实参 */
    private record PlanKey(Method declaration, Class<?> targetClass) {
    }

    /** {@code @CacheLoad} 注解及其声明方法（接口/父类/具体实现） */
    private record LoadBinding(CacheLoad annotation, Method declaration) {
    }

    private record MethodPlan(GenericCacheMetadata metadata,
                              JavaType valueType,
                              Expression keyExpression,
                              Expression condition,
                              Duration ttl,
                              boolean cacheNull) {
    }
}
