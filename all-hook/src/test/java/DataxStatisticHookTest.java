import com.github.thestyleofme.datax.hook.autoconfiguration.HookJpaConfiguration;
import com.github.thestyleofme.datax.hook.model.DataxStatistics;
import com.github.thestyleofme.datax.hook.repository.DataxStatisticsRepository;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/25 10:02
 * @since 1.0.0
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {
        HookJpaConfiguration.class
})
public class DataxStatisticHookTest {

    @Autowired
    private DataxStatisticsRepository dataxStatisticsRepository;

    @Test
    public void test() {
        // 单独测试请设置datax.home
        DataxStatistics dataxStatistics = new DataxStatistics();
        dataxStatistics.setJobId(1L);
        DataxStatistics save = dataxStatisticsRepository.save(dataxStatistics);
        Assert.assertNotNull(save.getId());
    }

}
