package com.xu.springbootbook09ch0902.batch;

import com.xu.springbootbook09ch0902.domain.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.validator.Validator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


//配置
@Configuration
@EnableBatchProcessing//开启批处理
public class CsvBatchConfig {

    @Bean
    public ItemReader<Person> reader() throws Exception {
        //使用FlatFileItemReader读取文件
        FlatFileItemReader<Person> reader = new FlatFileItemReader<>();
        //使用FlatFileItemReader的setResource方法设置csv文件的路径
        reader.setResource(new ClassPathResource("people.csv"));
        //csv文件的数据和领域模型做对应映射
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[]{"name", "age", "nation", "address"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

    //使用自己定义的CsvItemProcessor作为ItemProcessor处理器
    @Bean
    public ItemProcessor<Person, Person> processor() {
        CsvItemProcessor processor=new CsvItemProcessor();
        //给处理器设置校验
        processor.setValidator(csvBeanValidator());
        return processor;
    }

    @Bean
    public Validator<Person> csvBeanValidator(){
        return new CsvBeanValidator<Person>();
    }

    @Bean
    public ItemWriter<Person> writer(DataSource dataSource){//spring 能让容器中已有的bean以参数形式注入，spring boot已为我们定义了DataSource
        //使用jdbc批处理的JdbcBatchItemWriter来写数据到数据库
        JdbcBatchItemWriter<Person> writer=new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(
                new BeanPropertyItemSqlParameterSourceProvider<Person>()
        );
        String sql="insert into person "+"(name,age,nation,address)"+"" +
                "values(:name,:age,:nation,:address)";
        writer.setSql(sql);//设置要执行批处理的sql语句
        writer.setDataSource(dataSource);
        return writer;
    }

    //JobRepository的定义需要dataSource，transactionManager。spring boot已为我们自动配置了这两个类
    @Bean
    public JobRepository jobRepository(DataSource dataSource, PlatformTransactionManager transactionManager) throws Exception{
        JobRepositoryFactoryBean jobRepositoryFactoryBean=new JobRepositoryFactoryBean();
        jobRepositoryFactoryBean.setDataSource(dataSource);
        jobRepositoryFactoryBean.setTransactionManager(transactionManager);
        jobRepositoryFactoryBean.setDatabaseType("mysql");
        return jobRepositoryFactoryBean.getObject();
    }

    @Bean
    public SimpleJobLauncher jobLauncher(DataSource dataSource,PlatformTransactionManager transactionManager) throws  Exception{
        SimpleJobLauncher jobLauncher=new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository(dataSource,transactionManager));
        return jobLauncher;
    }

    //定义job
    @Bean
    public Job importJob(JobBuilderFactory jobs, Step s1){
        return jobs.get("importJob")
                .incrementer(new RunIdIncrementer())
                .flow(s1)//为job指定step
                .end()
                .listener(csvJobListener())//绑定监听器csvJobListener
                .build();
    }

    @Bean
    public CsvJobListener csvJobListener(){
        return new CsvJobListener();
    }

    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory,ItemReader<Person>
                      reader,ItemWriter<Person> writer,ItemProcessor<Person,Person> processor){
        return stepBuilderFactory.get("step1")
                .<Person,Person>chunk(65000)//批处理每次提交65000条数据
                .reader(reader)//绑定reader
                .processor(processor)//绑定processor
                .writer(writer)//绑定writer
                .build();
    }
}
