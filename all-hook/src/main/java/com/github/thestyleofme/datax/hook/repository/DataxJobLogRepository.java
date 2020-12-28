package com.github.thestyleofme.datax.hook.repository;

import com.github.thestyleofme.datax.hook.model.DataxJobLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/25 9:40
 * @since 1.0.0
 */
public interface DataxJobLogRepository extends JpaRepository<DataxJobLog, Long>, JpaSpecificationExecutor<DataxJobLog> {


}
